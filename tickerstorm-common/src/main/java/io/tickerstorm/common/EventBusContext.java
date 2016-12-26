package io.tickerstorm.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.eventbus.Destinations;

@Configuration
@ComponentScan("io.tickerstorm.common")
public class EventBusContext {

  private final static Logger logger = LoggerFactory.getLogger(EventBusContext.class);

  // Internal messages bus
  /**
   * Internal feed of market data which may come from brokers or historical queries. This is what
   * model pipelines listen to
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Bean
  public EventBus buildRealtimeEventBus() {
    return new EventBus(Destinations.REALTIME_MARKETDATA_BUS);
  }
  
  /**
   * Market data streaming realtime from brokers
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Bean
  public EventBus buildBrokerFeed() {
    return new EventBus(Destinations.BROKER_MARKETDATA_BUS);
  }

  /**
   * Topic for commands to be executed by other components of the infrastrucutre. Commands
   * frequently come from clients.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.COMMANDS_BUS)
  @Bean
  public EventBus buildCommandsEventBus() {
    //return new AsyncEventBus(Destinations.COMMANDS_BUS, executor);
    return new EventBus(Destinations.COMMANDS_BUS);
  }

  /**
   * Bus on which notificaitons are sent back to listeners from various components in the
   * infrastrucutre. Often as a response to commeands.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Bean
  public EventBus buildNotificaitonEventBus() {
    //return new AsyncEventBus(Destinations.NOTIFICATIONS_BUS, executor);
    return new EventBus(Destinations.NOTIFICATIONS_BUS);
  }


  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Bean
  public EventBus buildModelDataEventBus() {
    //return new AsyncEventBus(Destinations.MODEL_DATA_BUS, executor);
    return new EventBus(Destinations.MODEL_DATA_BUS);
  }

  /**
   * Feed of fields and market data reversed from model pipeline and processed by the retro model
   * pipeline.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  @Bean
  public EventBus buildRetroModelDataEventBus() {
    //return new AsyncEventBus(Destinations.RETRO_MODEL_DATA_BUS, executor);
    return new EventBus(Destinations.RETRO_MODEL_DATA_BUS);
  }

//  @Qualifier("eventBus")
//  @Bean
//  public Executor buildExecutorService() {
//    return Executors.newFixedThreadPool(4);
//  }


}
