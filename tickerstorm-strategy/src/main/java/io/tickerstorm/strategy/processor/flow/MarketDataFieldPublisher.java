package io.tickerstorm.strategy.processor.flow;

import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.processor.BaseEventProcessor;

@Component
public class MarketDataFieldPublisher extends BaseEventProcessor {

  @Subscribe
  public void handle(MarketData md) {

    md.getFields().stream().forEach(f -> {
      publish(f);
    });
  }

}
