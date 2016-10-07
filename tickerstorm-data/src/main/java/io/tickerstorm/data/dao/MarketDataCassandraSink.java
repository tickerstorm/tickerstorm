package io.tickerstorm.data.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;

@DependsOn(value = {"cassandraSetup"})
@Repository
public class MarketDataCassandraSink extends BaseCassandraSink<MarketDataDto> {

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(MarketDataCassandraSink.class);

  @Override
  protected int batchSize() {
    return 1999;
  }

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  private EventBus historicalBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @PreDestroy
  public void destroy() {
    super.destroy();
    historicalBus.unregister(this);
  }

  @PostConstruct
  public void init() {
    super.init();
    historicalBus.register(this);
  }

  @Subscribe
  public void onData(MarketData md) {
    batch(MarketDataDto.convert(md));
  }

  protected void persist(Collection<MarketDataDto> data) {

    if (null == data || data.isEmpty())
      return;

    List<List<?>> rows = new ArrayList<>();

    try {
      synchronized (data) {

        logger.debug(
            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");

        String insert = "INSERT INTO " + keyspace + ".marketdata "
            + "(symbol, date, type, source, interval, timestamp, hour, min, ask, asksize, bid, bidsize, close, high, low, open, price, properties, quantity, volume) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

        data.stream().forEach(r -> {
          rows.add(Lists.newArrayList(r.primarykey.symbol, r.primarykey.date, r.primarykey.type, r.primarykey.stream, r.primarykey.interval,
              r.primarykey.timestamp, r.primarykey.hour, r.primarykey.min, r.ask, r.askSize, r.bid, r.bidSize, r.close, r.high, r.low,
              r.open, r.price, r.properties, r.quantity, r.volume));
        });

        session.ingest(insert, rows);

        Map<String, Integer> streamCounts = countEntries(data);

        for (Entry<String, Integer> e : streamCounts.entrySet()) {
          Notification marker = new Notification(e.getKey());
          marker.addMarker(Markers.MARKET_DATA.toString());
          marker.addMarker(Markers.SAVE.toString());
          marker.addMarker(Markers.SUCCESS.toString());
          marker.expect = e.getValue();
          notificationsBus.post(marker);
        }
      }

    } catch (InvalidTypeException e) {

      for (List<?> md : rows) {
        logger.debug(Joiner.on(", ").skipNulls().join(md.iterator()));
      }

      logger.error(e.getMessage(), e);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  private Map<String, Integer> countEntries(Collection<MarketDataDto> data) {
    Map<String, Integer> streamCounts = Maps.newHashMap();
    for (MarketDataDto dto : data) {
      Integer count = streamCounts.putIfAbsent(dto.primarykey.stream, 1);

      if (count != null)
        streamCounts.replace(dto.primarykey.stream, count++);
    }
    return streamCounts;
  }

}
