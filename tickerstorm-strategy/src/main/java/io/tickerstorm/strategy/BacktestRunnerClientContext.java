package io.tickerstorm.strategy;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import backtype.storm.contrib.jms.spout.JmsSpout;
import io.tickerstorm.data.CommonContext;
import io.tickerstorm.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.data.eventbus.Destinations;
import io.tickerstorm.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.strategy.spout.RealtimeDestinationProvider;
import io.tickerstorm.strategy.spout.StormJmsTupleProducer;
import net.engio.mbassy.bus.MBassador;

@EnableJms
@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.strategy"})
@PropertySource({"classpath:default.properties"})
@Import({CommonContext.class})
public class BacktestRunnerClientContext {

  public static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(BacktestRunnerClientContext.class);

  @Value("${jms.transport}")
  private String transport;

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.info("Creating Connection Factory");
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Bean
  public EventBusToJMSBridge buildQueryJmsBridge(
      @Qualifier("query") MBassador<HistoricalFeedQuery> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_QUERY, template);
  }

  @Bean
  public JmsTemplate buildRealtimeJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    return template;
  }

  @Qualifier("realtime")
  @Bean
  public JmsSpout buildJmsSpout(ConnectionFactory factory) throws Exception {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(new RealtimeDestinationProvider(factory,
        factory.createConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE)
            .createTopic(Destinations.TOPIC_REALTIME_MARKETDATA)));
    spout.setJmsTupleProducer(new StormJmsTupleProducer());
    spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout.setDistributed(true);
    spout.setRecoveryPeriod(1000);

    return spout;

  }
}
