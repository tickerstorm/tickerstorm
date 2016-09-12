package io.tickerstorm.common;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Throwables;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.entity.MarketData;

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
  public EventBus buildRealtimeEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(executor, buildExceptionHandler());
  }

  @Bean
  public SubscriberExceptionHandler buildExceptionHandler() {
    return new SubscriberExceptionHandler() {

      @Override
      public void handleException(Throwable exception, SubscriberExceptionContext context) {
        logger.error(exception.getMessage(), Throwables.getRootCause(exception));

      }
    };
  }

  /**
   * Enable market realtime data from broker to be directly streamed to internal realtime market
   * data bus so that models can act upon live data
   * 
   * @param source
   * @param listener
   * @return
   */
  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Bean
  public EventBusToEventBusBridge<MarketData> buildBrokerFeedEventBridge(
      @Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus source,
      @Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus listener) {
    EventBusToEventBusBridge<MarketData> bridge = new EventBusToEventBusBridge<MarketData>(source, listener);
    return bridge;
  }

  /**
   * Market data streaming realtime from brokers
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Bean
  public EventBus buildBrokerFeed(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(executor, buildExceptionHandler());
  }

  /**
   * Query bus to historical data feed service. If you need historical data, query for it on this
   * bus.
   * 
   * @param handler
   * @return
   */
//  @Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS)
//  @Bean
//  public EventBus buildQueryEventBus(@Qualifier("eventBus") Executor executor) {
//    return new AsyncEventBus(executor, buildExceptionHandler());
//  }

  /**
   * Topic for commands to be executed by other components of the infrastrucutre. Commands
   * frequently come from clients.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.COMMANDS_BUS)
  @Bean
  public EventBus buildCommandsEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(executor, buildExceptionHandler());
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
  public EventBus buildNotificaitonEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(executor, buildExceptionHandler());
  }


  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Bean
  public EventBus buildModelDataEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(executor, buildExceptionHandler());
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
  public EventBus buildRetroModelDataEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(executor, buildExceptionHandler());
  }

  @Qualifier("eventBus")
  @Bean
  public Executor buildExecutorService() {
    return Executors.newFixedThreadPool(4);
  }


}
