package io.tickerstorm.strategy;

import io.tickerstorm.data.jms.ByDestinationNameJmsResolver;
import io.tickerstorm.data.jms.Destinations;
import io.tickerstorm.strategy.spout.RealtimeDestinationProvider;
import io.tickerstorm.strategy.spout.StormJmsTupleProducer;
import io.tickerstorm.strategy.util.BacktestClock;
import io.tickerstorm.strategy.util.Clock;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import backtype.storm.contrib.jms.spout.JmsSpout;

@EnableJms
@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.strategy"})
@PropertySource({"classpath:default.properties"})
public class BacktestConfig {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(BacktestConfig.class);

  @Value("${jms.transport}")
  private String transport;

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.info("Creating Connection Factory");
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Bean
  public JmsTemplate buildJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setDefaultDestinationName(Destinations.QUEUE_QUERY);
    template.setTimeToLive(2000);
    return template;
  }

  @Qualifier("realtime")
  @Bean
  public JmsSpout buildJmsSpout(ConnectionFactory factory) throws Exception {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(new RealtimeDestinationProvider(factory, factory.createConnection()
        .createSession(false, Session.CLIENT_ACKNOWLEDGE)
        .createTopic(Destinations.TOPIC_REALTIME_MARKETDATA)));
    spout.setJmsTupleProducer(new StormJmsTupleProducer());
    spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout.setDistributed(true);
    spout.setRecoveryPeriod(1000);

    return spout;

  }

  @Bean
  public Clock backtestClock() {
    return new BacktestClock();
  }
}
