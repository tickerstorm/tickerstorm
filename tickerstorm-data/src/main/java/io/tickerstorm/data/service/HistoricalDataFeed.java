package io.tickerstorm.data.service;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
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
import io.tickerstorm.common.data.query.HistoricalFeedQuery;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.dao.MarketDataDto;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@Repository
public class HistoricalDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(HistoricalDataFeed.class);

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  @Qualifier("notification")
  @Autowired
  private MBassador<Serializable> notificationBus;

  @Qualifier("query")
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

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
  public void onQuery(HistoricalFeedQuery query) {

    logger.debug("Historical feed query received");
    LocalDateTime start = query.from;
    LocalDateTime end = query.until;
    LocalDateTime date = start;

    Set<String> dates = new java.util.HashSet<>();
    dates.add(dateFormat.format(date));

    for (String s : query.symbols) {

      while (!date.equals(end)) {

        if (date.isBefore(end))
          date = date.plusDays(1);

        dates.add(dateFormat.format(date));
      }

      Select select = QueryBuilder.select().from("marketdata");
      select.where(QueryBuilder.eq("symbol", s.toLowerCase())).and(QueryBuilder.in("date", dates.toArray(new String[] {})))
          .and(QueryBuilder.eq("type", Candle.TYPE.toLowerCase())).and(QueryBuilder.eq("source", query.source.toLowerCase()))
          .and(QueryBuilder.eq("interval", query.periods.iterator().next()));

      logger.debug("Cassandra query: " + select.toString());
      long startTimer = System.currentTimeMillis();
      List<MarketDataDto> dtos = cassandra.select(select, MarketDataDto.class);
      logger.info("Query took " + (System.currentTimeMillis() - startTimer) + "ms to fetch " + dtos.size() + " results.");

      startTimer = System.currentTimeMillis();
      MarketData first = null;
      MarketData last = null;
      int count = 0;
      for (MarketDataDto dto : dtos) {

        try {
          MarketData m = dto.toMarketData(query.getStream());
                    
          if (null == first) {
            first = m;
            BaseMarker marker = new BaseMarker(query.id, query.getStream());
            marker.addMarker(Markers.QUERY_START.toString());
            marker.expect = dtos.size();
            notificationBus.publish(marker);
          }

          realtimeBus.publish(m);
          count++;

          if (count == dtos.size() && null == last) {
            last = m;
            BaseMarker marker = new BaseMarker(query.id, query.getStream());
            marker.addMarker(Markers.QUERY_END.toString());
            marker.expect = 0;
            notificationBus.publish(marker);
          }
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          // continue
        }
      }

      logger.info("Dispatch historical data feed took " + (System.currentTimeMillis() - startTimer) + "ms");

    }
  }
}
