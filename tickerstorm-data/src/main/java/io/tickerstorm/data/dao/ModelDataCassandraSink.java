package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
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
  private AsyncEventBus modelDataBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationsBus;

  @Override
  protected int batchSize() {
    return 4;
  }

  @Autowired
  private ModelDataDao dao;

  @PostConstruct
  public void init() {
    super.init();
    modelDataBus.register(this);
  }

  @PreDestroy
  public void destroy() {
    super.destroy();
    modelDataBus.unregister(this);
  }

  @Override
  protected Set<ModelDataDto> convert(Serializable data) {

    Set<ModelDataDto> dtos = new HashSet<>();

    if (Map.class.isAssignableFrom(data.getClass())) {

      Map<String, Field<?>> mps = (Map<String, Field<?>>) data;
      return ModelDataDto.convert(mps.values());

    } else if (Field.class.isAssignableFrom(data.getClass())) {

      return Sets.newHashSet(ModelDataDto.convert((Field<?>) data));

    } else if (MarketData.class.isAssignableFrom(data.getClass())) {

      return ModelDataDto.convert((MarketData) data);

    }

    return dtos;
  }

  @Override
  protected void persist(Collection<ModelDataDto> data) {
    try {
      synchronized (data) {

        logger.debug(
            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");

        List<List<?>> rows = new ArrayList<>();
        data.stream().forEach(r -> {
          rows.add(Lists.newArrayList(r.fields, r.primarykey.stream, r.primarykey.date, r.primarykey.timestamp));
        });

        session.ingest("UPDATE " + keyspace + ".modeldata SET fields = fields + ? WHERE stream = ? AND date = ? AND timestamp = ?;", rows);

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

  private Map<String, Integer> countEntries(Collection<ModelDataDto> data) {
    Map<String, Integer> streamCounts = Maps.newHashMap();
    for (ModelDataDto dto : data) {
      Integer count = streamCounts.putIfAbsent(dto.primarykey.stream, 1);

      if (count != null)
        streamCounts.replace(dto.primarykey.stream, (count + 1));
    }
    return streamCounts;
  }

};
