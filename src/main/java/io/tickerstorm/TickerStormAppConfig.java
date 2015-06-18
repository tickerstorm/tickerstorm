package io.tickerstorm;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

@ComponentScan(basePackages = { "io.tickerstorm" })
@Configuration
public class TickerStormAppConfig {

  @Qualifier("historical")
  @Bean
  public EventBus buildEventBus() {
    return new AsyncEventBus(Executors.newFixedThreadPool(2));
  }

}
