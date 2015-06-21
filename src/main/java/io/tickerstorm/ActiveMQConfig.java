package io.tickerstorm;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource({ "classpath:default.properties" })
public class ActiveMQConfig {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(ActiveMQConfig.class);

  @Value("${jms.transport}")
  private String transport;

  @Bean
  public BrokerService brokerService() throws Exception {
    logger.debug("Creating BrokerService");
    BrokerService broker = new BrokerService();
    broker.setUseShutdownHook(true);
    broker.addConnector(transport);
    return broker;

  }

  @Qualifier("historical")
  @Bean
  public MessageProducer buildHistoricalMarketDataPublisher(Session session, @Qualifier("historical") Destination destination)
      throws Exception {
    logger.debug("Creating Historcial Message Producer");
    MessageProducer producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    return producer;

  }

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.debug("Creating Connection Factory");
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @DependsOn({ "brokerService" })
  @Bean
  public Connection buildActiveMQConnection(ConnectionFactory connectionFactory) throws Exception {
    logger.debug("Creating Connection from ConnectorFactor");
    Connection connection = connectionFactory.createConnection();
    connection.start();
    return connection;
  }

  @Bean
  public Session buildActiveMQSession(Connection connection) throws Exception {
    logger.debug("Creating Session");
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    return session;
  }

  @Qualifier("historical")
  @Bean
  public Destination buildHistoricalMarketDataDestination(Session session) throws Exception {
    logger.debug("Creating Histrocal Destination");
    Destination destination = session.createTopic("topic.historical.marketdata");
    return destination;
  }

  @Qualifier("historical")
  @Bean
  public MessageConsumer buildHistorcialMarketDataConsumer(@Qualifier("historical") Destination destination, Session session)
      throws Exception {
    logger.debug("Creating MessageConsumer");
    MessageConsumer consumer = session.createConsumer(destination);
    return consumer;
  }

}
