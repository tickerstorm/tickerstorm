package io.tickerstorm.data;

import java.io.Serializable;
import java.util.Map;

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

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.IBusConfiguration;

@EnableJms
@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = {MarketDataDao.class, ModelDataDao.class})
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@ComponentScan(basePackages = {"io.tickerstorm.data"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class MarketDataApplication {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MarketDataApplication.class, args);
  }

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildEventBus(IBusConfiguration handler,
      @Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBusToEventBusBridge<MarketData> brokerFeed) {
    handler = handler.setProperty(net.engio.mbassy.bus.config.IBusConfiguration.Properties.BusId, "historical bus");
    MBassador<MarketData> historical = new MBassador<MarketData>(handler);
    brokerFeed.addListener(historical);// subscribe to broker data to record
    return historical;
  }

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildRealtimeJmsBridge(@Qualifier(Destinations.REALTIME_MARKETDATA_BUS) MBassador<MarketData> eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template);
  }

  @Bean
  public EventBusToJMSBridge buildRetroModelDataJmsBridge(
      @Qualifier(Destinations.RETRO_MODEL_DATA_BUS) MBassador<Map<String, Object>> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_RETRO_MODEL_DATA, template);
  }

  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) MBassador<Serializable> eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template);
  }


  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS) MBassador<DataFeedQuery> queryBus,
      @Qualifier(Destinations.MODEL_DATA_BUS) AsyncEventBus modelDataBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setQueryBus(queryBus);
    bridge.setModelDataBus(modelDataBus);
    return bridge;
  }
}
