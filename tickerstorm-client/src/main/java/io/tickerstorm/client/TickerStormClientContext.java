package io.tickerstorm.client;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import com.appx.h2o.H2ORestClient;
import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JMSToEventBusBridge;

@EnableJms
@SpringBootApplication
@ComponentScan(basePackages = {"io.tickerstorm.client"})
@PropertySource({"classpath:default.properties"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class TickerStormClientContext {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(TickerStormClientContext.class);

  public static final String SERVICE = "client";

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TickerStormClientContext.class, args);
  }

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildCommandsJmsBridge(@Qualifier(Destinations.COMMANDS_BUS) EventBus eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_COMMANDS, template, SERVICE);
  }

  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildNotificationsEventBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus bus,
      @Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus realtimeBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge( SERVICE);
    bridge.notificationBus = bus;
    bridge.realtimeBus = realtimeBus;
    return bridge;
  }

  @Bean
  public H2ORestClient buildRestClient() {

    H2ORestClient client = new H2ORestClient("http://localhost:54321");
    return client;

  }

}
