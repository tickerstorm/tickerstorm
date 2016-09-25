package io.tickerstorm.data;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;

import io.tickerstorm.ServiceLauncher;
import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;

@Configuration
@Import({EventBusContext.class, JmsEventBusContext.class})
public class IntegrationTestContext implements ApplicationListener<ContextRefreshedEvent> {

  public static final String SERVICE = "integration-test";

  @Bean
  public static PropertySourcesPlaceholderConfigurer buildConfig() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  @PreDestroy
  public void destroy() {
    ServiceLauncher.killMarketDataService();
    ServiceLauncher.killStrategyService();
  }

  // SENDERS
  @Qualifier(Destinations.COMMANDS_BUS)
  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.COMMANDS_BUS) EventBus eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_COMMANDS, template, SERVICE);
  }

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Bean
  public EventBusToJMSBridge buildBrokerFeedJmsBridge(@Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_REALTIME_BROKERFEED, template, SERVICE);
  }

  // RECEIVERS
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Bean
  public JMSToEventBusBridge buildNotificationBus(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus notificaitonsBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge(SERVICE);
    bridge.notificationBus = notificaitonsBus;
    return bridge;
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {
    ServiceLauncher.launchMarketDataService(true, 4000, "/tmp/tickerstorm/data-service/monitor");
    ServiceLauncher.launchStrategyService(true, 4001);

    try {
      Thread.sleep(5000);
    } catch (Exception e) {
      Throwables.propagate(e);
    }

  }

}
