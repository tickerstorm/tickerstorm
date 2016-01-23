package io.tickerstorm.data.service;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.data.query.ModelDataQuery;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.data.dao.ModelDataDto;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@Repository
public class ModelDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat =
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZ");

  private static final java.time.format.DateTimeFormatter dateFormat2 = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(ModelDataFeed.class);

  @Qualifier("notification")
  @Autowired
  private MBassador<Serializable> notificationBus;

  @Qualifier("query")
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

  @Qualifier("modelData")
  @Autowired
  private MBassador<Map<String, Object>> modeldataBus;

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
    LocalDateTime start = query.from;
    LocalDateTime end = query.until;
    LocalDateTime date = start;

    Set<String> dates = new java.util.HashSet<>();
    dates.add(dateFormat2.format(date));

    while (!date.equals(end)) {

      if (date.isBefore(end))
        date = date.plusDays(1);

      dates.add(dateFormat2.format(date));
    }

    Select select = QueryBuilder.select().from("modeldata");
    select.where(QueryBuilder.eq("modelname", query.stream.toLowerCase())).and(QueryBuilder.in("date", dates.toArray(new String[] {})))
        .and(QueryBuilder.gte("timestamp", dateFormat.format(query.from)))
        .and(QueryBuilder.lte("timestamp", dateFormat.format(query.until))).orderBy(QueryBuilder.desc("timestamp"));

    logger.debug("Cassandra query: " + select.toString());
    long startTimer = System.currentTimeMillis();
    List<ModelDataDto> dtos = cassandra.select(select, ModelDataDto.class);
    logger.info("Query took " + (System.currentTimeMillis() - startTimer) + "ms to fetch " + dtos.size() + " results.");

    startTimer = System.currentTimeMillis();
    Map<String, Object> first = null;
    Map<String, Object> last = null;
    int count = 0;
    for (ModelDataDto dto : dtos) {

      try {
        Map<String, Object> m = dto.fromRow();

        if (null == first) {
          first = m;
          BaseMarker marker = new BaseMarker(query.id, query.stream);
          marker.addMarker(Markers.QUERY_START.toString());
          marker.expect = dtos.size();
          notificationBus.publish(marker);
        }

        modeldataBus.publish(m);
        count++;

        if (count == dtos.size() && null == last) {
          last = m;
          BaseMarker marker = new BaseMarker(query.id, query.stream);
          marker.addMarker(Markers.QUERY_END.toString());
          marker.expect = 0;
          notificationBus.publish(marker);
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        // continue
      }
    }

    logger.info("Dispatch model data feed took " + (System.currentTimeMillis() - startTimer) + "ms");


  }

}
