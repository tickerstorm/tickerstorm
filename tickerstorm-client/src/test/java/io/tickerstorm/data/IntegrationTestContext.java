package io.tickerstorm.data;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JmsToEventBusBridge;
import javax.annotation.PreDestroy;
import javax.jms.ConnectionFactory;
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

@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.client.data"})
@PropertySource({"classpath:default.properties"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class IntegrationTestContext implements ApplicationListener<ContextRefreshedEvent> {

  public static final String SERVICE = "integration-test";

  private ServiceLauncher launcher = new ServiceLauncher();

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
  public EventBusToJMSBridge buildCommandDataProducer(@Qualifier(Destinations.COMMANDS_BUS) EventBus eventbus, ConnectionFactory template)
      throws Exception {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_COMMANDS, template, SERVICE);
  }

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Bean
  public EventBusToJMSBridge buildBrokerFeedProducer(@Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus eventbus,
      ConnectionFactory template) throws Exception {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_REALTIME_BROKERFEED, template, SERVICE);
  }

  // CONSUMERS
  @Bean
  public JmsToEventBusBridge buildNotificationDataConsumer(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus modelDataBus, ConnectionFactory factory)
      throws Exception {
    return new JmsToEventBusBridge(factory, modelDataBus, Destinations.TOPIC_NOTIFICATIONS);
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {
    launcher.launchMarketDataService(true, 4000, "/tmp/tickerstorm/data-service/monitor");
    launcher.launchStrategyService(true, 4001);
  }
}
