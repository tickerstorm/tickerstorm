package io.tickerstorm.strategy.processor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.strategy.util.CacheManager;
import io.tickerstorm.strategy.util.Clock;
import net.sf.ehcache.Element;

@Component
public abstract class BaseEventProcessor {

  private final static String CACHE = "md-cache";
  
  private final static Logger logger = LoggerFactory.getLogger(BaseEventProcessor.class);

  @Qualifier("eventBus")
  @Autowired
  private AsyncEventBus eventBus;

  @PostConstruct
  protected void init() {
    eventBus.register(this);
  }

  @PreDestroy
  protected void destroy() {
    eventBus.unregister(this);
  }

  protected void publish(Object o) {
    eventBus.post(o);
  }
  
  @Subscribe
  protected void onCommand(Command command) throws Exception {
    
  }
  
  @Subscribe
  protected void onNotification(Notification notification) throws Exception {
    
  }
  
  protected Map<String, Set<Field<?>>> byType(MarketData md) throws Exception {

    Map<String, Set<Field<?>>> coll = new HashedMap();
    coll.put(Field.Name.CONTINOUS_FIELDS.field(), filterForFields(md, BigDecimal.class));
    coll.put(Field.Name.DISCRETE_FIELDS.field(), filterForFields(md, Integer.class));
    coll.put(Field.Name.CATEGORICAL_FIELDS.field(), filterForFields(md, String.class));
    coll.put(Field.Name.TEMPORAL_FIELDS.field(), filterForFields(md, Instant.class));
    return coll;
  }

  protected Set<Field<?>> filterForFields(MarketData md, Class<?> clazz) {

    HashSet<Field<?>> insts = new HashSet<>();

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().isAssignableFrom(clazz)) {
        insts.add((Field<?>) f);
      }
    }

    return insts;

  }

  protected List<Field<?>> cache(Field<?> f) {

    final String key = CacheManager.buildKey(f).toString();
    CacheManager.getInstance(CACHE).putIfAbsent(new Element(key, new ArrayList<Field<?>>()));
    List<Field<?>> previous = new ArrayList<>();

    try {

      CacheManager.getInstance(CACHE).acquireWriteLockOnKey(key);
      previous = (List<Field<?>>) CacheManager.getInstance(CACHE).get(key).getObjectValue();
      previous.add(f);
      CacheManager.getInstance(CACHE).put(new Element(key, previous));

    } finally {

      CacheManager.getInstance(CACHE).releaseWriteLockOnKey(key);

    }

    return previous;
  }

  protected SynchronizedDescriptiveStatistics cache(String key, Field<?> f, int period) {
    SynchronizedDescriptiveStatistics q = null;
    try {

      CacheManager.getInstance("md-cache").acquireWriteLockOnKey(key);
      CacheManager.getInstance("md-cache").putIfAbsent(new Element(key, new SynchronizedDescriptiveStatistics(period)));

      q = (SynchronizedDescriptiveStatistics) CacheManager.getInstance("md-cache").get(key).getObjectValue();

      if (f.getFieldType().isAssignableFrom(BigDecimal.class))
        q.addValue(((BigDecimal) f.getValue()).doubleValue());

      if (f.getFieldType().isAssignableFrom(Integer.class))
        q.addValue(((Integer) f.getValue()).doubleValue());

    } finally {
      CacheManager.getInstance("md-cache").releaseWriteLockOnKey(key);
    }

    return q;

  }

  @Autowired
  private Clock clock;
  
  protected Instant time(){
    return clock.now();
  }

}
