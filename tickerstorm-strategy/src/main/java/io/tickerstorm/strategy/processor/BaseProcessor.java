package io.tickerstorm.strategy.processor;

import java.util.HashMap;
import java.util.Map;
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

      if (command.config.get("transformers") != null) {
        Map<String, Object> trans = (Map) command.config.get("transformers");
        Map<String, Object> thisT = (Map) trans.get(name());
        getConfig(command.getStream()).putAll(thisT);
      }
    }

    if (sessionEnd.test(command)) {
      configs.remove(command.getStream());
    }

  }

  public abstract String name();

  @Subscribe
  public void onNotification(Notification notification) throws Exception {
    logger.debug("Notification received " + notification);
  }


}
