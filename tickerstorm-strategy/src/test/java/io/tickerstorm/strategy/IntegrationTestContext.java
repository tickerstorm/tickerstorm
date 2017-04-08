package io.tickerstorm.strategy;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JmsToEventBusBridge;
import javax.jms.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@Import({EventBusContext.class, JmsEventBusContext.class})
public class IntegrationTestContext {

  public static final String SERVICE = "strategy-test";

  @Autowired
  private GaugeService gaugeService;

  @Bean
  public static PropertySourcesPlaceholderConfigurer buildConfig() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  /**
   * Bus on which notificaitons are sent back to listeners from various components in the
   * infrastrucutre. Often as a response to commeands.
   */
  @Qualifier("TestNotificationBus")
  @Bean
  public EventBus buildNotificaitonEventBusTest() {
    return new EventBus(Destinations.NOTIFICATIONS_BUS);
  }

  /**
   * Bus on which notificaitons are sent back to listeners from various components in the
   * infrastrucutre. Often as a response to commeands.
   */
  @Qualifier("RealtimeMarketDataBusTest")
  @Bean
  public EventBus buildRealtimeDataBusTest() {
    return new EventBus(Destinations.REALTIME_MARKETDATA_BUS);
  }

  // SENDERS
  @Qualifier("RealtimeMarketDataBusTest")
  @Bean
  public EventBusToJMSBridge buildBrokerFeedJmsBridge(@Qualifier("RealtimeMarketDataBusTest") EventBus eventbus, ConnectionFactory template)
      throws Exception {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template, SERVICE);
  }

  // CONSUMERS
  @Qualifier("JMSToEventBusBridgeTest")
  @Bean
  public JmsToEventBusBridge buildBrokerDataJmsBridge(@Qualifier("TestNotificationBus") EventBus modelDataBus, ConnectionFactory factory)
      throws Exception {
    return new JmsToEventBusBridge(factory, modelDataBus, Destinations.TOPIC_NOTIFICATIONS);
  }
}
