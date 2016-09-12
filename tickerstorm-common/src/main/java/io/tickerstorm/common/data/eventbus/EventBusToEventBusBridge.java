package io.tickerstorm.common.data.eventbus;

import javax.annotation.PreDestroy;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class EventBusToEventBusBridge<T> {

  private EventBus source;
  private EventBus[] listeners;

  public EventBusToEventBusBridge(EventBus source, EventBus[] listeners) {
    this.source = source;
    this.source.register(this);
    this.listeners = listeners;
  }

  public EventBusToEventBusBridge(EventBus source, EventBus listener) {
    this.source = source;
    this.source.register(this);
    this.listeners = new EventBus[] {listener};
  }

  public void addListener(EventBus bus) {
    synchronized (this.listeners) {
      this.listeners = Lists.asList(bus, this.listeners).toArray(new EventBus[] {});
    }
  }

  @Subscribe
  public void onMessage(T o) {
    synchronized (this.listeners) {
      for (EventBus l : listeners) {
        l.post(o);
      }
    }
  }

  @PreDestroy
  public void destroy() {
    this.source.unregister(this);
  }

}
