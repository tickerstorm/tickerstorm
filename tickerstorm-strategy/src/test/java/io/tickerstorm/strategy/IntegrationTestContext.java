package io.tickerstorm.strategy;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JMSToEventBusBridge;

@Configuration
@Import({EventBusContext.class, JmsEventBusContext.class})
public class IntegrationTestContext {

  public static final String SERVICE = "strategy-test";

  @Bean
  public static PropertySourcesPlaceholderConfigurer buildConfig() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  /**
   * Bus on which notificaitons are sent back to listeners from various components in the
   * infrastrucutre. Often as a response to commeands.
   * 
   * @param handler
   * @return
   */
  @Qualifier("TestNotificationBus")
  @Bean
  public EventBus buildNotificaitonEventBusTest() {
    return new EventBus(Destinations.NOTIFICATIONS_BUS);
  }

  /**
   * Bus on which notificaitons are sent back to listeners from various components in the
   * infrastrucutre. Often as a response to commeands.
   * 
   * @param handler
   * @return
   */
  @Qualifier("RealtimeMarketDataBusTest")
  @Bean
  public EventBus buildRealtimeDataBusTest() {
    return new EventBus(Destinations.REALTIME_MARKETDATA_BUS);
  }

  // SENDERS
  @Qualifier("RealtimeMarketDataBusTest")
  @Bean
  public EventBusToJMSBridge buildBrokerFeedJmsBridge(@Qualifier("RealtimeMarketDataBusTest") EventBus eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template, SERVICE);
  }

  // RECEIVERS
  @Qualifier("JMSToEventBusBridgeTest")
  @Bean
  public JMSToEventBusBridge buildNotificationBusTest(@Qualifier("TestNotificationBus") EventBus notificaitonsBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge(SERVICE);
    bridge.notificationBus = notificaitonsBus;
    return bridge;
  }

}
