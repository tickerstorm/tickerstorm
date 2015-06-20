package io.tickerstorm;

import io.tickerstorm.dao.MarketDataDao;

import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

@ComponentScan(basePackages = { "io.tickerstorm" })
@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = { "classpath:/META-INF/spring/cassandra-beans.xml" })
@PropertySource({ "classpath:default.properties" })
public class TickerStormAppConfig {

  @Value("${jms.transport}")
  private String transport;

  @Qualifier("historical")
  @Bean
  public EventBus buildEventBus() {
    return new AsyncEventBus(Executors.newFixedThreadPool(2));
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TickerStormAppConfig.class, args);
  }

  @Bean
  public BrokerService brokerService() throws Exception {

    BrokerService broker = new BrokerService();
    broker.setUseShutdownHook(true);
    broker.addConnector(transport);
    return broker;

  }

  @Qualifier("historical")
  @Bean
  public MessageProducer buildHistoricalMarketDataPublisher(Session session) throws Exception {

    Destination destination = session.createTopic("topic.historical.marketdata");

    // Create a MessageProducer from the Session to the Topic or Queue
    MessageProducer producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    return producer;

  }

  @Bean
  public Session buildActiveMQConnection() throws Exception {
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    Connection connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    return session;
  }

  @Qualifier("historical")
  @Bean
  public MessageConsumer buildHistorcialMarketDataConsumer(Session session) throws Exception {

    Destination destination = session.createTopic("topic.historical.marketdata");
    MessageConsumer consumer = session.createConsumer(destination);

    return consumer;
  }

}
