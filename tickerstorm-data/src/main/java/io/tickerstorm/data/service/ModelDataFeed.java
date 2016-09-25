package io.tickerstorm.data.service;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.DeleteData;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.ModelDataQuery;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.data.dao.ModelDataDto;

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
  private CassandraOperations cassandra;

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
  public void onDelete(DeleteData delete) {
    if (!StringUtils.isEmpty(delete.stream) && delete.markers.contains(Markers.MODEL_DATA.toString())) {
      dao.deleteByStream(delete.stream);

      Notification notif = new Notification(delete.id, delete.stream);
      notif.markers.add(Markers.MODEL_DATA.toString());
      notif.markers.add(Markers.DELETED.toString());
      notificationBus.post(notif);
    }
  }

  @Subscribe
  public void onQuery(ModelDataQuery query) {

    logger.debug("Model data feed query received");

    List<ModelDataDto> dtos = dao.findByStreamAndTimestampIsBetween(query.stream, query.from, query.until);

    final AtomicInteger count = new AtomicInteger();

    dtos.stream().forEach(d -> {
      count.addAndGet(d.fields.size());
    });

    long startTimer = System.currentTimeMillis();

    Notification marker = new Notification(query.id, query.stream);
    marker.addMarker(Markers.QUERY.toString());
    marker.addMarker(Markers.START.toString());
    marker.expect = count.get();
    notificationBus.post(marker);

    dtos.stream().forEach(d -> {
      d.asFields().stream().forEach(f -> {
        modeldataBus.post(f);
      });
    });

    marker = new Notification(query.id, query.stream);
    marker.addMarker(Markers.QUERY.toString());
    marker.addMarker(Markers.END.toString());
    marker.expect = 0;
    notificationBus.post(marker);


    logger.info("Dispatch model data feed took " + (System.currentTimeMillis() - startTimer) + "ms");


  }

}
