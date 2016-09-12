package io.tickerstorm.strategy.processor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.data.eventbus.Destinations;

@Component
public abstract class BaseEventProcessor extends BaseProcessor {

  @Autowired
  protected GaugeService gauge;

  @Qualifier("processorEventBus")
  @Autowired
  protected EventBus eventBus;

  @Autowired
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  private EventBus realtimeBus;

  @PreDestroy
  @Override
  protected void destroy() {
    super.destroy();
    eventBus.unregister(this);
    realtimeBus.unregister(this);
  }

  @PostConstruct
  @Override
  protected void init() {
    super.init();
    eventBus.register(this);
    realtimeBus.register(this);
  }

  protected void publish(Object o) {
    eventBus.post(o);
  }

}
