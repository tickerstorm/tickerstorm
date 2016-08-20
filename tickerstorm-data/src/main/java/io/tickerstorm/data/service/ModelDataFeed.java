package io.tickerstorm.data.service;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.data.query.ModelDataQuery;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.data.dao.ModelDataDto;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@Repository
public class ModelDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat = java.time.format.DateTimeFormatter.ISO_DATE_TIME;

  private static final java.time.format.DateTimeFormatter dateFormat2 = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(ModelDataFeed.class);

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationBus;

  @Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS)
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private AsyncEventBus modeldataBus;

  @Autowired
  private CassandraOperations cassandra;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @PostConstruct
  public void setup() {
    queryBus.subscribe(this);
  }

  @PreDestroy
  public void destroy() {
    queryBus.unsubscribe(this);
  }

  @Handler
  public void onQuery(ModelDataQuery query) {

    logger.debug("Model data feed query received");
    LocalDateTime start = query.from.atOffset(ZoneOffset.UTC).toLocalDateTime();
    LocalDateTime end = query.until.atOffset(ZoneOffset.UTC).toLocalDateTime();
    LocalDateTime date = start;

    Set<Integer> dates = new java.util.HashSet<>();
    dates.add(Integer.valueOf(dateFormat2.format(date)));

    while (!date.equals(end)) {

      if (date.isBefore(end))
        date = date.plusDays(1);

      dates.add(Integer.valueOf(dateFormat2.format(date)));
    }

    Select select = QueryBuilder.select().from("modeldata");
    select.where(QueryBuilder.eq("stream", query.stream.toLowerCase())).and(QueryBuilder.in("date", dates.toArray(new Integer[] {})))
        .and(QueryBuilder.gte("timestamp", dateFormat.format(query.from)))
        .and(QueryBuilder.lte("timestamp", dateFormat.format(query.until)));

    logger.debug("Cassandra query: " + select.toString());
    long startTimer = System.currentTimeMillis();
    List<ModelDataDto> dtos = cassandra.select(select, ModelDataDto.class);
    logger.info("Query took " + (System.currentTimeMillis() - startTimer) + "ms to fetch " + dtos.size() + " results.");

    final AtomicInteger count = new AtomicInteger();

    dtos.stream().forEach(d -> {
      count.addAndGet(d.fields.size());
    });

    startTimer = System.currentTimeMillis();

    BaseMarker marker = new BaseMarker(query.id, query.stream);
    marker.addMarker(Markers.QUERY_START.toString());
    marker.expect = count.get();
    notificationBus.publish(marker);

    dtos.stream().forEach(d -> {
      d.asFields().stream().forEach(f -> {
        modeldataBus.post(f);
      });
    });

    marker = new BaseMarker(query.id, query.stream);
    marker.addMarker(Markers.QUERY_END.toString());
    marker.expect = 0;
    notificationBus.publish(marker);


    logger.info("Dispatch model data feed took " + (System.currentTimeMillis() - startTimer) + "ms");


  }

}
