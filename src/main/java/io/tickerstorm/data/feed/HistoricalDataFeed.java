package io.tickerstorm.data.feed;

import io.tickerstorm.data.dao.MarketDataDto;
import io.tickerstorm.data.jms.Destinations;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

@Repository
@Listener(references = References.Strong)
public class HistoricalDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(HistoricalDataFeed.class);

  @Qualifier("query")
  @Autowired
  private MBassador<HistoricalFeedQuery> queryBus;

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  @Autowired
  private CassandraOperations cassandra;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @PostConstruct
  public void init() {
    queryBus.subscribe(this);
  }

  @PreDestroy
  public void destroy() {
    queryBus.unsubscribe(this);
  }

  @JmsListener(destination = Destinations.QUEUE_QUERY)
  @Handler
  public void onQuery(@Payload HistoricalFeedQuery query) {

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

      logger.info("Fetched " + dtos.size() + " results.");

      for (MarketDataDto dto : dtos) {
        realtimeBus.post(dto.toMarketData()).asynchronously();
      }

    }
  }
}
