package io.tickerstorm.strategy.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.CompletionTracker;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.eventbus.Destinations;

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

    if (CompletionTracker.Session.isStart.test(command)) {

      if (command.config.containsKey("transformers")) {

        List<Map<String, String>> trans = (List<Map<String, String>>) command.config.get("transformers");

        trans.stream().filter(m -> m.containsKey("transformer") && m.get("transformer").equalsIgnoreCase(name())).forEach(m -> {
          getConfig(command.getStream()).putAll(m);
        });
      }

      // Initialize cache
      CacheManager.getInstance(command.getStream());

      Notification not = new Notification(command);
      not.markers.add(Markers.SUCCESS.toString());
      not.properties.put("transformer", name());
      notificationsBus.post(not);

    }

    if (CompletionTracker.Session.isEnd.test(command)) {
      configs.remove(command.getStream());
    }

  }

  public abstract String name();

  @Subscribe
  public void onNotification(Notification notification) throws Exception {
    logger.debug("Notification received " + notification);
  }


}
