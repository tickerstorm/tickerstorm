package io.tickerstorm.strategy.processor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.common.entity.Session;

public abstract class BaseProcessor implements Processor {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Autowired
  @Qualifier(Destinations.COMMANDS_BUS)
  private EventBus commandsBus;

  protected final Map<String, Map<String, Object>> configs = new HashMap<>();

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  protected Map<String, Set<Field<?>>> byType(MarketData md) throws Exception {

    Map<String, Set<Field<?>>> coll = new HashMap<>();
    coll.put(Field.Name.CONTINOUS_FIELDS.field(), filterForFields(md, BigDecimal.class));
    coll.put(Field.Name.DISCRETE_FIELDS.field(), filterForFields(md, Integer.class));
    coll.put(Field.Name.CATEGORICAL_FIELDS.field(), filterForFields(md, String.class));
    coll.put(Field.Name.TEMPORAL_FIELDS.field(), filterForFields(md, Instant.class));
    return coll;
  }

  protected Map<String, Object> getConfig(String stream) {

    Map<String, Object> config = (Map) this.configs.putIfAbsent(stream, Maps.newHashMap());
    return this.configs.get(stream);

  }

  protected void addToConfiguration(String stream, String key, Object value) {
    getConfig(stream).put(key, value);
  }

  @PreDestroy
  protected void destroy() {
    commandsBus.unregister(this);
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

  @PostConstruct
  protected void init() {
    commandsBus.register(this);
  }

  protected void notify(Notification not) {
    notificationsBus.post(not);
  }

  @Subscribe
  public void onCommand(Command command) throws Exception {

    logger.debug("Command received " + command);

    Predicate<Command> sessionStart = (p -> command.markers.contains(Session.SESSION_START));
    Predicate<Command> sessionEnd = (p -> command.markers.contains(Session.SESSION_END));

    if (sessionStart.test(command)) {
      getConfig(command.getStream()).putAll(command.config);
    }

    if (sessionEnd.test(command)) {
      configs.remove(command.getStream());
    }

  }

  @Subscribe
  public void onNotification(Notification notification) throws Exception {
    logger.debug("Notification received " + notification);
  }


}
