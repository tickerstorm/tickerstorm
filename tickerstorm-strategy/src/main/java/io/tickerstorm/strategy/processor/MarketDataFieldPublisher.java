package io.tickerstorm.strategy.processor;

import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.MarketData;

@Component
public class MarketDataFieldPublisher extends BaseEventProcessor {

  @Subscribe
  public void handle(MarketData md) {

    md.getFields().stream().forEach(f -> {
      publish(f);
    });
  }

}
