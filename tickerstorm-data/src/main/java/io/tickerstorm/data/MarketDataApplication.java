package io.tickerstorm.data;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;

@EnableJms
@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = {MarketDataDao.class, ModelDataDao.class})
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@ComponentScan(basePackages = {"io.tickerstorm.data"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class MarketDataApplication {

  public static final String SERVICE = "data-service";

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MarketDataApplication.class, args);
  }

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Bean
  public EventBus buildEventBus(@Qualifier("eventBus") Executor executor) {
    AsyncEventBus historical = new AsyncEventBus(Destinations.HISTORICL_MARKETDATA_BUS, executor);
    return historical;
  }

  // SENDERS
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Bean
  public EventBusToJMSBridge buildRealtimeJmsBridge(@Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template, SERVICE);
  }

  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  @Bean
  public EventBusToJMSBridge buildRetroModelDataJmsBridge(@Qualifier(Destinations.RETRO_MODEL_DATA_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_RETRO_MODEL_DATA, template, SERVICE);
  }

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template, SERVICE);
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
  public EventBusToEventBusBridge<MarketData> buildBrokerFeedEventBridge(@Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus source,
      @Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus listener,
      @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS) EventBus historical) {
    EventBusToEventBusBridge<MarketData> bridge = new EventBusToEventBusBridge<MarketData>(source, listener);
    bridge.addListener(historical);// subscribe to broker data to record
    return bridge;
  }

  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier(Destinations.MODEL_DATA_BUS) EventBus modelDataBus,
      @Qualifier(Destinations.COMMANDS_BUS) EventBus commandBus, @Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus brokerFeedBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge(SERVICE);
    bridge.modelDataBus = modelDataBus;
    bridge.commandsBus = commandBus;
    bridge.brokerFeedBus = brokerFeedBus;
    return bridge;
  }
}
