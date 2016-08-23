package io.tickerstorm.strategy.processor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.AsyncEventBus;

@Component
public abstract class BaseEventProcessor extends BaseProcessor {

  protected final String CACHE = getClass().getSimpleName() + "-cache";

  @Qualifier("processorEventBus")
  @Autowired
  protected AsyncEventBus eventBus;

  @PreDestroy
  protected void destroy() {
    eventBus.unregister(this);
  }

  @PostConstruct
  protected void init() {
    eventBus.register(this);
  }

  protected void publish(Object o) {
    eventBus.post(o);
  }

  @Override
  protected String getCacheKey() {
    return CACHE;
  }

}
