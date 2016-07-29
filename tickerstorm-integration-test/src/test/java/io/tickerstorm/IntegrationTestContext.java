package io.tickerstorm;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import net.engio.mbassy.bus.MBassador;

@Configuration
@Import({EventBusContext.class, JmsEventBusContext.class})
@PropertySource({"classpath:default.properties"})
public class IntegrationTestContext {

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.BROKER_MARKETDATA_BUS) MBassador<Serializable> eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.BROKER_MARKETDATA_BUS, template);
  }

  @Bean
  public EventBusToJMSBridge buildHistoricalQueryJmsBridge(
      @Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS) MBassador<Serializable> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_HISTORICAL_DATA_QUERY, template);
  }


  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) MBassador<Serializable> notificaitonsBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setNotificationBus(notificaitonsBus);
    return bridge;
  }

}
