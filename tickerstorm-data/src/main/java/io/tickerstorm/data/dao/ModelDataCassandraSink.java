package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@Repository
@Listener(references = References.Strong)
public class ModelDataCassandraSink extends BaseCassandraSink {

  @Qualifier("modelData")
  @Autowired
  private MBassador<Map<String, Object>> modelDataBus;

  @Autowired
  private ModelDataDao dao;

  @PostConstruct
  public void init() {
    super.init();
    modelDataBus.subscribe(this);
  }

  @PreDestroy
  public void destroy() {
    super.destroy();
    modelDataBus.unsubscribe(this);
  }

  @Override
  protected Serializable convert(Serializable data) {
    return ModelDataDto.convert((Map<String, Object>) data);
  }

  @Override
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
