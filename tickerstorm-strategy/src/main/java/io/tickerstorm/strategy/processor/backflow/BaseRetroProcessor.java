package io.tickerstorm.strategy.processor.backflow;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.strategy.processor.BaseProcessor;

@Component
public abstract class BaseRetroProcessor extends BaseProcessor {

  protected final String CACHE = getClass().getSimpleName() + "-retro-cache";

  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  @Autowired
  protected AsyncEventBus retroEventBus;

  @PreDestroy
  protected void destroy() {
    retroEventBus.unregister(this);
  }

  @PostConstruct
  protected void init() {
    retroEventBus.register(this);
  }

  protected void publish(Object o) {
    retroEventBus.post(o);
  }

  @Override
  protected String getCacheKey() {
    return CACHE;
  }

}
