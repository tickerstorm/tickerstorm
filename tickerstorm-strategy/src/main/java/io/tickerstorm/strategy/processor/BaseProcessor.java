package io.tickerstorm.strategy.processor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.strategy.util.CacheManager;
import io.tickerstorm.strategy.util.Clock;
import net.engio.mbassy.bus.MBassador;
import net.sf.ehcache.Element;

public abstract class BaseProcessor implements Processor {
  
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationsBus;
  
  @Autowired
  private Clock clock;

  protected final Map<String, Map<String, String>> configs = new HashMap<>();
  
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected Map<String, Set<Field<?>>> byType(MarketData md) throws Exception {

    Map<String, Set<Field<?>>> coll = new HashedMap();
    coll.put(Field.Name.CONTINOUS_FIELDS.field(), filterForFields(md, BigDecimal.class));
    coll.put(Field.Name.DISCRETE_FIELDS.field(), filterForFields(md, Integer.class));
    coll.put(Field.Name.CATEGORICAL_FIELDS.field(), filterForFields(md, String.class));
    coll.put(Field.Name.TEMPORAL_FIELDS.field(), filterForFields(md, Instant.class));
    return coll;
  }

  protected Map<String, String> configuration(String stream) {
    if (this.configs.containsKey(stream))
      return this.configs.get(stream);

    return Maps.newHashMap();
  }

  protected void configure(String stream, Map<String, String> map) {
    configs.put(stream, map);
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
  
  protected abstract String getCacheKey();
  
  protected List<Field<?>> cache(Field<?> f, int period) {

    final String key = CacheManager.buildKey(f).toString() + "-p" + period;
    List<Field<?>> previous = Lists.newArrayList(f);
    Element e = CacheManager.getInstance(getCacheKey()).putIfAbsent(new Element(key, previous));

    if (e != null) {
      try {
        CacheManager.getInstance(getCacheKey()).acquireWriteLockOnKey(key);

        previous = (List<Field<?>>) e.getObjectValue();
        previous.add(f);

        CacheManager.getInstance(getCacheKey()).replace(new Element(key, previous));

      } finally {
        CacheManager.getInstance(getCacheKey()).releaseWriteLockOnKey(key);
      }

      Collections.sort(previous, Field.SORT_REVERSE_TIMESTAMP);
    }

    return previous;
  }
  
  @Override
  @Subscribe
  public void onCommand(Command command) throws Exception {

    logger.debug("Command received " + command);

    Predicate<Command> sessionStart = (p -> command.markers.contains(Markers.SESSION_START.toString()));
    Predicate<Command> sessionEnd = (p -> command.markers.contains(Markers.SESSION_END.toString()));

    if (sessionStart.test(command)) {

      if (this.configs.containsKey(command.getStream()))
        configuration(command.getStream()).putAll(command.config);
      else
        configure(command.getStream(), command.config);
    }

    if (sessionEnd.test(command)) {
      configs.remove(command.getStream());
    }

  }

  protected List<Field<?>> fetch(Field<?> key, int period) {

    Element e = CacheManager.getInstance(getCacheKey()).get(CacheManager.buildKey(key).toString() + "-p" + period);

    if (e == null)
      return Lists.newArrayList();

    List<Field<?>> previous = (List<Field<?>>) e.getObjectValue();

    Collections.sort(previous, Field.SORT_REVERSE_TIMESTAMP);
    return previous;
  }
  
  protected void notify(Notification not) {
    notificationsBus.post(not);
  }
  
  protected Instant time() {
    return clock.now();
  }
  
  @Override
  @Subscribe
  public void onNotification(Notification notification) throws Exception {
    logger.debug("Notification received " + notification);
  }

}
