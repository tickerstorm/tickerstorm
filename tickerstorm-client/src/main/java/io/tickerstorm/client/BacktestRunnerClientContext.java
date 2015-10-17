package io.tickerstorm.client;

import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

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

import io.tickerstorm.common.data.CommonContext;
import io.tickerstorm.common.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import io.tickerstorm.common.data.feed.HistoricalFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

@EnableJms
@SpringBootApplication
@ComponentScan(basePackages = {"io.tickerstorm.client"})
@PropertySource({"classpath:default.properties"})
@Import({CommonContext.class})
public class BacktestRunnerClientContext {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(BacktestRunnerClientContext.class);

  public static void main(String[] args) throws Exception {
    SpringApplication.run(BacktestRunnerClientContext.class, args);
  }

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildQueryJmsBridge(@Qualifier("query") MBassador<HistoricalFeedQuery> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_QUERY, template);
  }

  @Bean
  public EventBusToJMSBridge buildCommandsJmsBridge(@Qualifier("commands") MBassador<Serializable> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_COMMANDS, template);
  }


  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildRealtimeEventBridge(@Qualifier("realtime") MBassador<MarketData> realtimeBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setRealtimeBus(realtimeBus);
    return bridge;
  }

  @Bean
  public JMSToEventBusBridge buildNotificationsEventBridge(@Qualifier("notification") MBassador<Serializable> bus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setNotificationBus(bus);
    bridge.setExplodeCollections(true);
    return bridge;
  }

  @Bean
  public JmsTemplate buildRealtimeJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    template.setPubSubNoLocal(true);
    return template;
  }



}
