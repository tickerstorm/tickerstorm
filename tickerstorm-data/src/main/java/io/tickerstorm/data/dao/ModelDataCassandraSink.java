package io.tickerstorm.data.dao;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.export.ModelDataExporter;
import net.sf.ehcache.Element;

@DependsOn(value = {"cassandraSetup"})
@Repository
public class ModelDataCassandraSink extends BaseCassandraSink<ModelDataDto> {

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(ModelDataCassandraSink.class);

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Autowired
  private ModelDataDao dao;

  @Override
  protected int batchSize() {
    return 1999;
  }

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

  @Subscribe
  public void onData(MarketData md) {

    md.getFields().stream().forEach(f -> {
      cacheFieldName(f);
    });

    batch(ModelDataDto.convert(md));

  }

  @Subscribe
  public void onData(Collection<Field<?>> fs) {

    fs.stream().forEach(f -> {
      cacheFieldName(f);
    });
    batch(ModelDataDto.convert(fs));
  }

  @Subscribe
  public void onData(Field<?> f) {

    cacheFieldName(f);
    batch(ModelDataDto.convert(f));

  }

  private void cacheFieldName(Field<?> f) {

    Element e = CacheManager.getInstance(f.getStream())
        .putIfAbsent(new Element(ModelDataExporter.CACHE_KEY_SUFFIX, new ConcurrentSkipListSet<>(Lists.newArrayList(f.getName()))));

    if (e != null) {

      try {
        CacheManager.getInstance(f.getStream()).acquireWriteLockOnKey(ModelDataExporter.CACHE_KEY_SUFFIX);
        ((Set) CacheManager.getInstance(f.getStream()).get(ModelDataExporter.CACHE_KEY_SUFFIX).getObjectValue()).add(f.getName());
      } finally {
        CacheManager.getInstance(f.getStream()).releaseWriteLockOnKey(ModelDataExporter.CACHE_KEY_SUFFIX);
      }
    }
  }

  @Override
  protected void persist(Collection<ModelDataDto> data) {

    if (null == data || data.isEmpty())
      return;

    try {

      dao.ingest(data);

      for (Entry<String, Integer> e : countEntries(data).entrySet()) {

        logger.debug("Persisting " + e.getValue() + " records for stream " + e.getKey() + ", " + count.addAndGet(data.size())
            + " total saved and " + received.get() + " received");

        Notification marker = new Notification(e.getKey());
        marker.addMarker(Markers.MODEL_DATA.toString());
        marker.addMarker(Markers.SAVE.toString());
        marker.addMarker(Markers.SUCCESS.toString());
        marker.expect = e.getValue();
        notificationsBus.post(marker);

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
