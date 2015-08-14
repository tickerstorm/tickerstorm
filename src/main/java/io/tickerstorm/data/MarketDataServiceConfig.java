package io.tickerstorm.data;

import io.tickerstorm.data.messaging.Destinations;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.support.destination.DynamicDestinationResolver;

@EnableJms
@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.data, io.tickerstorm.messaging"})
@PropertySource({"classpath:default.properties"})
@Import({CommonConfig.class})
public class MarketDataServiceConfig {



  public static final Logger logger = org.slf4j.LoggerFactory
      .getLogger(MarketDataServiceConfig.class);

  @Value("${jms.transport}")
  private String transport;

  @Bean(initMethod = "start", destroyMethod = "stop")
  public BrokerService brokerService() throws Exception {
    logger.info("Creating BrokerService");
    BrokerService broker = new BrokerService();
    broker.setUseShutdownHook(true);
    broker.addConnector(transport);
    return broker;
  }

  @DependsOn(value = {"brokerService"})
  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.info("Creating Connection Factory");
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Qualifier("realtime")
  @Bean
  public JmsTemplate getRealtimeJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new DynamicDestinationResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setDefaultDestinationName(Destinations.TOPIC_REALTIME_MARKETDATA);
    return template;
  }

  @Qualifier("query")
  @Bean
  public DefaultMessageListenerContainer buildQueryListenerContainer(ConnectionFactory factory) {
    DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
    container.setConnectionFactory(factory);
    container.setDestinationName(Destinations.QUEUE_QUERY);
    container.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    container.setDestinationResolver(new DynamicDestinationResolver());
    return container;
  }

}
