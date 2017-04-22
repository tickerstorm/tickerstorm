package io.tickerstorm.data.service;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.ModelDataQuery;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDto;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class ModelDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat = java.time.format.DateTimeFormatter.ISO_DATE_TIME;

  private static final java.time.format.DateTimeFormatter dateFormat2 =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));

  private static final Logger logger = LoggerFactory.getLogger(ModelDataFeed.class);

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus queryBus;

  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  @Autowired
  private EventBus modeldataBus;

  @Autowired
  private ModelDataDao dao;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @PostConstruct
  public void setup() {
    queryBus.register(this);
  }

  @PreDestroy
  public void destroy() {
    queryBus.unregister(this);
  }

  @Subscribe
  public void onDelete(Command delete) {
    if (delete.markers.contains(Markers.MODEL_DATA.toString()) && delete.markers.contains(Markers.DELETE.toString())) {
      dao.deleteByStream(delete.getStream());

      Notification notif = new Notification(delete);
      notif.markers.add(Markers.SUCCESS.toString());
      notificationBus.post(notif);
    }
  }

  @Subscribe
  public void onQuery(ModelDataQuery query) {

    logger.debug("Model data feed query received " + query);

    List<CassandraModelDataDto> dtos = dao.findByStreamAndTimestampIsBetween(query.getStream(), query.from, query.until);

    final AtomicInteger count = new AtomicInteger();

    dtos.stream().forEach(d -> {
      count.addAndGet(d.fields.size());
    });

    long startTimer = System.currentTimeMillis();

    Notification marker = new Notification(query);
    marker.addMarker(Markers.START.toString());
    marker.expect = count.get();
    notificationBus.post(marker);

    dtos.stream().forEach(d -> {
      d.asFields().stream().forEach(f -> {
        modeldataBus.post(f);
      });
    });

    logger.info("Dispatching " + dtos.size() + " model data fields took " + (System.currentTimeMillis() - startTimer) + "ms");

    marker = new Notification(query);
    marker.addMarker(Markers.END.toString());
    marker.expect = 0;
    notificationBus.post(marker);


  }

}
