package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@DependsOn(value={"cassandraSetup"})
@Repository
@Listener(references = References.Strong)
public class ModelDataCassandraSink extends BaseCassandraSink<ModelDataDto> {
  
  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(ModelDataCassandraSink.class);

  @Qualifier("modelData")
  @Autowired
  private MBassador<Map<String, Object>> modelDataBus;

  private PreparedStatement insert;

  @Override
  protected int batchSize() {
    return 4;
  }

  @Autowired
  private ModelDataDao dao;

  @PostConstruct
  public void init() {
    super.init();
    modelDataBus.subscribe(this);

    insert =
        session.getSession().prepare(new SimpleStatement("INSERT INTO modeldata (stream, date, timestamp, fields) VALUES (?, ?, ?, ?);"));
  }

  @PreDestroy
  public void destroy() {
    super.destroy();
    modelDataBus.unsubscribe(this);
  }

  @Override
  protected Serializable convert(Serializable data) {

    Serializable s = null;

    try {
      s = ModelDataDto.convert((Map<String, Object>) data);
    } catch (IllegalArgumentException e) {
      // nothing
    }

    return s;
  }

  /*
   * @Override protected void persist(List<Object> data) { try { synchronized (data) { logger.debug(
   * "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and "
   * + received.get() + " received"); dao.save((List) data); }
   * 
   * } catch (Exception e) { logger.error(e.getMessage(), e); } }
   */

  @Override
  protected void persist(List<ModelDataDto> data) {
    try {
      synchronized (data) {
        
        dao.save(data);
        
//        logger.debug(
//            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");
//
//        BatchStatement batch = new BatchStatement();
//        for (ModelDataDto o : data) {
//          batch.add(insert.bind(o.primarykey.stream, BigInteger.valueOf(o.primarykey.date), o.primarykey.timestamp, o.fields));
//        }
//
//        org.springframework.cassandra.core.Cancellable c = session.executeAsynchronously(batch, listener);
      
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  protected void persist(ModelDataDto data) {
    try {

      logger.debug("Persisting model data records, " + count.addAndGet(1) + " total saved and " + received.get() + " received");
      dao.save((ModelDataDto) data);

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

}
