package io.tickerstorm.data;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.client.ServiceLauncher;
import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JMSToEventBusBridge;

@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.client"})
@PropertySource({"classpath:default.properties"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class IntegrationTestContext implements ApplicationListener<ContextRefreshedEvent> {

  public static final String SERVICE = "integration-test";

  @Autowired
  private ServiceLauncher launcher;

  @Bean
  public static PropertySourcesPlaceholderConfigurer buildConfig() {
    return new PropertySourcesPlaceholderConfigurer();
  }


  @PreDestroy
  public void destroy() throws Exception {
    launcher.killMarketDataService();
    launcher.killStrategyService();
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
  @Bean
  public JMSToEventBusBridge buildNotificationBus(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus notificaitonsBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge(SERVICE);
    bridge.notificationBus = notificaitonsBus;
    return bridge;
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {
    launcher.launchMarketDataService(true, 4000, "/tmp/tickerstorm/data-service/monitor");
    launcher.launchStrategyService(true, 4001);
  }
}
