package io.tickerstorm.data.dao;

import java.io.Serializable;
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

import com.google.common.collect.Maps;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@DependsOn(value = {"cassandraSetup"})
@Repository
@Listener(references = References.Strong)
public class MarketDataCassandraSink extends BaseCassandraSink<MarketDataDto> {

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(MarketDataCassandraSink.class);

  @Autowired
  private MarketDataDao dao;

  @Override
  protected int batchSize() {
    return 149;
  }

  @Qualifier("historical")
  @Autowired
  private MBassador<MarketData> historicalBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationsBus;

  @PreDestroy
  public void destroy() {
    super.destroy();
    historicalBus.unsubscribe(this);
  }

  @PostConstruct
  public void init() {
    super.init();
    historicalBus.subscribe(this);
  }

  @Override
  protected Serializable convert(Serializable data) {

    if (MarketData.class.isAssignableFrom(data.getClass())) {
      return MarketDataDto.convert((MarketData) data);
    }

    return data;
  }

  protected void persist(List<MarketDataDto> data) {
    try {
      synchronized (data) {
        logger.debug(
            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");
        dao.save((List<MarketDataDto>) data);

        Map<String, Integer> streamCounts = countEntries(data);

        for (Entry<String, Integer> e : streamCounts.entrySet()) {
          BaseMarker marker = new BaseMarker(e.getKey());
          marker.addMarker(Markers.MARKET_DATA_SAVED.toString());
          marker.expect = e.getValue();
          notificationsBus.publishAsync(marker);
        }
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  private Map<String, Integer> countEntries(List<MarketDataDto> data) {
    Map<String, Integer> streamCounts = Maps.newHashMap();
    for (MarketDataDto dto : data) {
      Integer count = streamCounts.putIfAbsent(dto.primarykey.source, 1);

      if (count != null)
        streamCounts.replace(dto.primarykey.source, count++);
    }
    return streamCounts;
  }

}
