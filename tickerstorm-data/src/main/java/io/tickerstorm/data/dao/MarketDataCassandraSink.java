package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@Repository
@Listener(references = References.Strong)
public class MarketDataCassandraSink extends BaseCassandraSink {

  @Autowired
  private MarketDataDao dao;

  @Qualifier("historical")
  @Autowired
  private MBassador<MarketData> historicalBus;

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

  protected void persist(List<Object> data) {
    try {
      synchronized (data) {
        dao.save((List) data);        
        logger.debug(
            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }
}
