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
    
//    md.getFields().stream().forEach(f -> {
//      publish(f);
//    });
    
    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));
    logger.debug("Market Data Field Processor took :" + (System.currentTimeMillis() - start) + "ms");
  }

}
