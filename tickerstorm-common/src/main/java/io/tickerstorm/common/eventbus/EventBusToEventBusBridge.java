package io.tickerstorm.common.eventbus;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class EventBusToEventBusBridge<T> {
  
  private static final Logger logger = LoggerFactory.getLogger(EventBusToEventBusBridge.class);

  private final EventBus source;
  private final List<EventBus> listeners = new ArrayList<EventBus>();

  public EventBusToEventBusBridge(EventBus source, EventBus[] listeners) {
    this.source = source;
    this.source.register(this);
    this.listeners.addAll(Lists.newArrayList(listeners));
  }

  public EventBusToEventBusBridge(EventBus source, EventBus listener) {
    this.source = source;
    this.source.register(this);
    this.listeners.add(listener);
  }

  public void addListener(EventBus bus) {
    synchronized (this.listeners) {
      this.listeners.add(bus);
    }
  }

  @Subscribe
  public void onMessage(T o) {
    for (EventBus l : listeners) {
      logger.debug("Bridging message " + o.toString() + " from " + source.identifier() + " to " + l.identifier());
      l.post(o);
    }
  }

  @PreDestroy
  public void destroy() {
    this.source.unregister(this);
  }

}
