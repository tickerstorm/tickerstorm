package io.tickerstorm.data.feed;

import io.tickerstorm.data.dao.MarketDataDto;
import io.tickerstorm.entity.Candle;

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
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

@Repository
public class HistoricalDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(HistoricalDataFeed.class);

  @Qualifier("query")
  @Autowired
  private EventBus bus;

  @Qualifier("realtime")
  @Autowired
  private EventBus realtime;

  @Autowired
  private CassandraOperations cassandra;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @PostConstruct
  public void init() {
    bus.register(this);
  }

  @PreDestroy
  public void destroy() {
    bus.unregister(this);
  }

  @Subscribe
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
      select.where(QueryBuilder.eq("symbol", s.toLowerCase()))
          .and(QueryBuilder.in("date", dates.toArray(new String[] {})))
          .and(QueryBuilder.eq("type", Candle.TYPE.toLowerCase()))
          .and(QueryBuilder.eq("source", query.source.toLowerCase()))
          .and(QueryBuilder.eq("interval", query.periods.iterator().next()));

      logger.debug(select.toString());

      List<MarketDataDto> dtos = cassandra.select(select, MarketDataDto.class);

      logger.debug("Fetched " + dtos.size() + " results.");

      for (MarketDataDto dto : dtos) {
        realtime.post(dto.toMarketData());
      }
    }
  }
}
