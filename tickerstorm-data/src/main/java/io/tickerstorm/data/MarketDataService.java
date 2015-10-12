package io.tickerstorm.data;

import java.net.URI;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import io.tickerstorm.data.dao.MarketDataDao;


@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@ComponentScan(basePackages = {"io.tickerstorm.data"})
@Import({CommonContext.class})
public class MarketDataService {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MarketDataService.class, args);
  }

  @Value("${jms.transport}")
  protected String transport;

  @Bean(initMethod = "start", destroyMethod = "stop", name = "jmsBroker")
  public BrokerService startActiveMQ() throws Exception {
    BrokerService broker = new BrokerService();
    broker.setBrokerName("tickerstorm");
    TransportConnector connector = new TransportConnector();
    connector.setUri(new URI(transport));
    broker.addConnector(connector);
    broker.setPersistent(false);;
    return broker;
  }

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }
}
