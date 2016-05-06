package io.tickerstorm.common.data.eventbus;

import javax.annotation.PreDestroy;

import com.google.common.collect.Lists;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;

@Listener
public class EventBusToEventBusBridge<T> {

  private MBassador<T> source;
  private MBassador<T>[] listeners;

  public EventBusToEventBusBridge(MBassador<T> source, MBassador<T>[] listeners) {
    this.source = source;
    this.source.subscribe(this);
    this.listeners = listeners;
  }

  public EventBusToEventBusBridge(MBassador<T> source, MBassador<T> listener) {
    this.source = source;
    this.source.subscribe(this);
    this.listeners = new MBassador[]{listener};
  }

  public void addListener(MBassador<T> bus) {
    synchronized (this.listeners) {
      this.listeners = Lists.asList(bus, this.listeners).toArray(new MBassador[] {});
    }
  }

  @Handler
  public void onMessage(T o) {
    synchronized (this.listeners) {
      for (MBassador<T> l : listeners) {
        l.publish(o);
      }
    }
  }

  @PreDestroy
  public void destroy() {
    this.source.unsubscribe(this);
    this.listeners = null;
  }

}
