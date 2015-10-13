package io.tickerstorm.data;

import java.net.URI;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.data.eventbus.Destinations;
import io.tickerstorm.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.data.eventbus.JMStoQueryEventBusBridge;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.common.Properties;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;


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
  public JmsTemplate buildJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    return template;
  }

  @Bean
  public JMStoQueryEventBusBridge buildQueryBridge() {
    return new JMStoQueryEventBusBridge();
  }

  @Qualifier("historical")
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildEventBus(IPublicationErrorHandler handler) {
    return new MBassador<MarketData>(new BusConfiguration().addFeature(Feature.SyncPubSub.Default())
        .addFeature(Feature.AsynchronousHandlerInvocation.Default(1, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default())
        .addPublicationErrorHandler(handler).setProperty(Properties.Common.Id, "historical bus"));
  }

  @Bean
  public EventBusToJMSBridge buildRealtimeJmsBridge(
      @Qualifier("realtime") MBassador<MarketData> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template);
  }

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }
}
