package io.tickerstorm.common;

import java.io.Serializable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.engio.mbassy.bus.config.IBusConfiguration;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@Configuration
@ComponentScan("io.tickerstorm.common")
public class EventBusContext {


  @Bean
  public BusConfiguration busConfiguration(IPublicationErrorHandler handler) {
    return new BusConfiguration().addFeature(Feature.SyncPubSub.Default()).addFeature(Feature.AsynchronousHandlerInvocation.Default(2, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default()).addPublicationErrorHandler(handler);
  }

  // Internal messages bus
  /**
   * Internal feed of market data which may come from brokers or historical queries. This is what
   * model pipelines listen to
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildRealtimeEventBus(IBusConfiguration handler) {
    handler = handler.setProperty(IBusConfiguration.Properties.BusId, Destinations.REALTIME_MARKETDATA_BUS);
    MBassador<MarketData> bus = new MBassador<MarketData>(handler);
    return bus;
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
      @Qualifier(Destinations.BROKER_MARKETDATA_BUS) MBassador<MarketData> source,
      @Qualifier(Destinations.REALTIME_MARKETDATA_BUS) MBassador<MarketData> listener) {
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
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildBrokerFeed(IBusConfiguration handler) {
    handler = handler.setProperty(IBusConfiguration.Properties.BusId, Destinations.BROKER_MARKETDATA_BUS);
    return new MBassador<MarketData>(handler);
  }

  /**
   * Query bus to historical data feed service. If you need historical data, query for it on this
   * bus.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<DataFeedQuery> buildQueryEventBus(IBusConfiguration handler) {
    handler = handler.setProperty(IBusConfiguration.Properties.BusId, Destinations.HISTORICAL_DATA_QUERY_BUS);
    return new MBassador<DataFeedQuery>(handler);
  }

  /**
   * Topic for commands to be executed by other components of the infrastrucutre. Commands
   * frequently come from clients.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.COMMANDS_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<Serializable> buildCommandsEventBus(IBusConfiguration handler) {
    handler = handler.setProperty(IBusConfiguration.Properties.BusId, Destinations.COMMANDS_BUS);
    return new MBassador<Serializable>(handler);
  }

  /**
   * Bus on which notificaitons are sent back to listeners from various components in the
   * infrastrucutre. Often as a response to commeands.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<Serializable> buildNotificaitonEventBus(IBusConfiguration handler) {
    handler = handler.setProperty(IBusConfiguration.Properties.BusId, Destinations.NOTIFICATIONS_BUS);
    return new MBassador<Serializable>(handler);
  }

  /**
   * Feed of fields and market data transformed and processed by the forward topology of the model
   * pipeline.
   * 
   * @param handler
   * @return
   */
  // @Qualifier(Destinations.MODEL_DATA_BUS)
  // @Bean(destroyMethod = "shutdown")
  // public MBassador<Map<String, Object>> buildModelDataEventBus(IBusConfiguration handler) {
  // handler = handler.setProperty(IBusConfiguration.Properties.BusId, Destinations.MODEL_DATA_BUS);
  // return new MBassador<Map<String, Object>>(handler);
  // }

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Bean
  public AsyncEventBus buildModelDataEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(Destinations.MODEL_DATA_BUS, executor);
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
  public AsyncEventBus buildRetroModelDataEventBus(@Qualifier("eventBus") Executor executor) {
    return new AsyncEventBus(Destinations.RETRO_MODEL_DATA_BUS, executor);
  }

  @Qualifier("eventBus")
  @Bean
  public Executor buildExecutorService() {
    return Executors.newFixedThreadPool(4);
  }


}
