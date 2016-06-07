package io.tickerstorm.common;

import java.io.Serializable;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

import io.tickerstorm.common.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@EnableJms
@Configuration
@ComponentScan("io.tickerstorm.common")
@PropertySource({"classpath:default.properties"})
public class CommonContext {

  @Value("${jms.transport}")
  protected String transport;

  @Bean
  public BusConfiguration busConfiguration(IPublicationErrorHandler handler) {
    return new BusConfiguration().addFeature(Feature.SyncPubSub.Default()).addFeature(Feature.AsynchronousHandlerInvocation.Default(2, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default()).addPublicationErrorHandler(handler);
  }

  // Internal messages bus
  /**
   * Internal feed of market data which may come from brokers of historical queries. This is what
   * model pipelines listen to
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildRealtimeEventBus(BusConfiguration handler,
      @Qualifier(Destinations.BROKER_MARKETDATA_BUS) MBassador<MarketData> brokerFeed) {
    MBassador<MarketData> bus = new MBassador<MarketData>(handler);
    return bus;
  }

  @Qualifier("brokerfeed")
  @Bean
  public EventBusToEventBusBridge<MarketData> buildBrokerFeedEventBridge(
      @Qualifier(Destinations.BROKER_MARKETDATA_BUS) MBassador<MarketData> source, @Qualifier("realtime") MBassador<MarketData> listener) {
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
  public MBassador<MarketData> buildBrokerFeed(BusConfiguration handler) {
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
  public MBassador<DataFeedQuery> buildQueryEventBus(BusConfiguration handler) {
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
  public MBassador<Serializable> buildCommandsEventBus(BusConfiguration handler) {
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
  public MBassador<Serializable> buildNotificaitonEventBus(BusConfiguration handler) {
    return new MBassador<Serializable>(handler);
  }

  /**
   * Feed of fields and market data transformed and processed by the forward topology of the model
   * pipeline.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<Map<String, Object>> buildModelDataEventBus(BusConfiguration handler) {
    return new MBassador<Map<String, Object>>(handler);
  }

  /**
   * Feed of fields and market data reversed from model pipeline and processed by the retro model
   * pipeline.
   * 
   * @param handler
   * @return
   */
  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<Map<String, Object>> buildRetroModelDataEventBus(BusConfiguration handler) {
    return new MBassador<Map<String, Object>>(handler);
  }

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Bean
  public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory cf) {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    factory.setConnectionFactory(cf);
    factory.setDestinationResolver(new ByDestinationNameJmsResolver());
    factory.setConcurrency("1");
    factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    return factory;
  }
}
