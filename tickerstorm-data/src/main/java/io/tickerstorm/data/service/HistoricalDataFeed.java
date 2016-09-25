package io.tickerstorm.data.service;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

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
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.HistoricalFeedQuery;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.data.dao.MarketDataDto;

@Repository
public class HistoricalDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));

  private static final Logger logger = LoggerFactory.getLogger(HistoricalDataFeed.class);

  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Autowired
  private EventBus realtimeBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus queryBus;

  @Autowired
  private CassandraOperations cassandra;

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
  public void onQuery(HistoricalFeedQuery query) {

    logger.debug("Historical feed query received");
    Instant start = query.from.toInstant(ZoneOffset.UTC);
    Instant end = query.until.toInstant(ZoneOffset.UTC);
    Instant date = start;

    List<BigInteger> dates = new ArrayList<>();
    dates.add(new BigInteger(dateFormat.format(date)));

    while (date.compareTo(end) < 0) {
      date = date.plus(1, ChronoUnit.DAYS);
      dates.add(new BigInteger(dateFormat.format(date)));
    }

    for (String s : query.symbols) {

      Select select = QueryBuilder.select().from(keyspace, "marketdata");
      select.where(QueryBuilder.eq("symbol", s.toLowerCase())).and(QueryBuilder.in("date", dates))
          .and(QueryBuilder.eq("type", Candle.TYPE.toLowerCase())).and(QueryBuilder.eq("source", query.stream.toLowerCase()))
          .and(QueryBuilder.eq("interval", query.periods.iterator().next()));

      logger.debug("Cassandra query: " + select.toString());
      long startTimer = System.currentTimeMillis();
      List<MarketDataDto> dtos = cassandra.select(select, MarketDataDto.class);
      logger.info("Query took " + (System.currentTimeMillis() - startTimer) + "ms to fetch " + dtos.size() + " results.");

      Notification marker = new Notification(query.id, query.getStream());
      marker.addMarker(Markers.QUERY.toString());
      marker.addMarker(Markers.START.toString());
      marker.expect = dtos.size();
      notificationBus.post(marker);

      startTimer = System.currentTimeMillis();
      dtos.stream().map(d -> {
        return d.toMarketData(query.getStream());
      }).forEach(m -> {
        realtimeBus.post(m);
      });

      marker = new Notification(query.id, query.getStream());
      marker.addMarker(Markers.QUERY.toString());
      marker.addMarker(Markers.END.toString());
      marker.expect = 0;
      notificationBus.post(marker);

      logger.debug("Dispatch historical data feed took " + (System.currentTimeMillis() - startTimer) + "ms");

    }
  }
}
