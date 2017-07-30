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
import io.tickerstorm.common.reactive.Observations;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.eventbus.Destinations;

public abstract class BaseProcessor implements Processor {

  public static final String TRANSFORMERS_YML_NODE = "transformers";
  public static final String TRANSFORMER_CONFIG_KEY = "transformer";
  protected final Map<String, Map<String, Object>> configs = new HashMap<>();
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  protected EventBus notificationsBus;
  @Autowired
  @Qualifier(Destinations.COMMANDS_BUS)
  private EventBus commandsBus;

  protected Map<String, Object> getConfig(String stream) {
    this.configs.putIfAbsent(stream, Maps.newHashMap());
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

    if (Observations.Session.isStart.test(command)) {
      logger.debug("Command received " + command);

      if (command.config != null && command.config.containsKey(TRANSFORMERS_YML_NODE)) {

        Map<String, List<Map<String, String>>> trans = (Map<String, List<Map<String, String>>>) command.config.get(TRANSFORMERS_YML_NODE);

        if (trans.containsKey(name())) {
          TransformerConfig config = new TransformerConfig(trans.get(name()));
          getConfig(command.getStream()).put(TRANSFORMER_CONFIG_KEY, config);
        }

        // Initialize cache
        CacheManager.getInstance(command.getStream());
      } else {
        getConfig(command.getStream()).put(TRANSFORMER_CONFIG_KEY, new TransformerConfig(false));
      }

      Notification not = new Notification(command);
      not.markers.add(Markers.SUCCESS.toString());
      not.properties.put("transformer", name());
      notificationsBus.post(not);

    }

    if (Observations.Session.isEnd.test(command)) {
      logger.debug("Command received " + command);
      configs.remove(command.getStream());

      Notification not = new Notification(command);
      not.markers.add(Markers.SUCCESS.toString());
      not.properties.put("transformer", name());
      notificationsBus.post(not);
    }

  }

  protected boolean isActive(String stream) {
    return getConfig(stream).get(TRANSFORMER_CONFIG_KEY) != null && ((TransformerConfig) getConfig(stream).get(TRANSFORMER_CONFIG_KEY)).isActive();
  }

  public abstract String name();

  @Subscribe
  public void onNotification(Notification notification) throws Exception {
    logger.debug("Notification received " + notification);
  }


}
