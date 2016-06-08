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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.Maps;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Markers;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@DependsOn(value = {"cassandraSetup"})
@Repository
@Listener(references = References.Strong)
public class ModelDataCassandraSink extends BaseCassandraSink<ModelDataDto> {

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(ModelDataCassandraSink.class);

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private MBassador<Map<String, Object>> modelDataBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationsBus;

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

  @Override
  protected void persist(List<ModelDataDto> data) {
    try {
      synchronized (data) {

        logger.debug(
            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");
        
        dao.save(data);
        
        Map<String, Integer> streamCounts = countEntries(data);

        for (Entry<String, Integer> e : streamCounts.entrySet()) {
          BaseMarker marker = new BaseMarker(e.getKey());
          marker.addMarker(Markers.MODEL_DATA_SAVED.toString());
          marker.expect = e.getValue();
          notificationsBus.publishAsync(marker);
        }

      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }
  
  private Map<String, Integer> countEntries(List<ModelDataDto> data){
    Map<String, Integer> streamCounts = Maps.newHashMap();
    for (ModelDataDto dto : data) {
      Integer count = streamCounts.putIfAbsent(dto.primarykey.stream, 1);

      if (count != null)
        streamCounts.replace(dto.primarykey.stream, (count+1));
    }
    return streamCounts;
  }

};
