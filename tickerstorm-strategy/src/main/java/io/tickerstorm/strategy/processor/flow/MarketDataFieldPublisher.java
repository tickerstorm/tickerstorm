package io.tickerstorm.strategy.processor.flow;

import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.processor.BaseEventProcessor;

@Component
public class MarketDataFieldPublisher extends BaseEventProcessor {

  public final static String METRIC_TIME_TAKEN = "metric.clock.time";

  @Subscribe
  public void handle(MarketData md) {

    long start = System.currentTimeMillis();
    publish(md.getFields());
    logger.trace("Market Data Field Processor took :" + (System.currentTimeMillis() - start) + "ms");
    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));

  }

  @Override
  public String name() {
    return "marketdata-field-publisher";
  }

}
