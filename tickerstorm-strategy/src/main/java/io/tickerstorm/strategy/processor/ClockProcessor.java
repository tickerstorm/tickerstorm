package io.tickerstorm.strategy.processor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.util.Clock;

@Component
public class ClockProcessor extends BaseEventProcessor {

  @Autowired
  private Clock clock;

  @Subscribe
  public void handle(MarketData data) {

    if (data != null) {
      clock.update(data.getTimestamp());
    }
  }
}
