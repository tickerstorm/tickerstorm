package io.tickerstorm.data;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

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
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.common.data.CommonContext;
import io.tickerstorm.common.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.common.Properties;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@EnableJms
@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = {MarketDataDao.class, ModelDataDao.class})
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@ComponentScan(basePackages = {"io.tickerstorm.data"})
@Import({CommonContext.class})
public class MarketDataApplicationContext {

  @Value("${jms.transport}")
  protected String transport;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MarketDataApplicationContext.class, args);
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
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
    template.setPubSubNoLocal(true);
    return template;
  }

  @Qualifier("historical")
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildEventBus(IPublicationErrorHandler handler) {
    return new MBassador<MarketData>(new BusConfiguration().addFeature(Feature.SyncPubSub.Default())
        .addFeature(Feature.AsynchronousHandlerInvocation.Default(1, 4)).addFeature(Feature.AsynchronousMessageDispatch.Default())
        .addPublicationErrorHandler(handler).setProperty(Properties.Common.Id, "historical bus"));
  }

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildRealtimeJmsBridge(@Qualifier("realtime") MBassador<MarketData> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template);
  }
  
  @Bean
  public EventBusToJMSBridge buildRetroModelDataJmsBridge(@Qualifier("retroModelData") MBassador<Map<String, Object>> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_RETRO_MODEL_DATA, template);
  }

  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier("notification") MBassador<Serializable> eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template);
  }


  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier("query") MBassador<DataFeedQuery> queryBus,
      @Qualifier("commands") MBassador<Serializable> commandsBus, @Qualifier("modelData") MBassador<Map<String, Object>> modelDataBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setQueryBus(queryBus);
    bridge.setCommandsBus(commandsBus);
    bridge.setModelDataBus(modelDataBus);
    return bridge;
  }

}
