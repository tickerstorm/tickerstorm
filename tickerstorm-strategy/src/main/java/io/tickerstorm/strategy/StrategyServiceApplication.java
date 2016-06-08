package io.tickerstorm.strategy;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.jms.spout.JmsSpout;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;

import io.tickerstorm.common.CommonContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.strategy.bolt.FieldTypeSplittingBolt;
import io.tickerstorm.strategy.spout.CommandsTupleProducer;
import io.tickerstorm.strategy.spout.DestinationProvider;
import io.tickerstorm.strategy.spout.MarketDataTupleProducer;
import io.tickerstorm.strategy.spout.ModelDataMessageProducer;
import io.tickerstorm.strategy.spout.NotificationMessageProducer;
import io.tickerstorm.strategy.util.BacktestClock;
import io.tickerstorm.strategy.util.Clock;

@EnableJms
@SpringBootApplication
@ComponentScan(basePackages = {"io.tickerstorm.strategy"})
@PropertySource({"classpath:default.properties"})
@Import({CommonContext.class})
public class StrategyServiceApplication {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(StrategyServiceApplication.class);

  @Value("${jms.transport}")
  private String transport;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(StrategyServiceApplication.class, args);
  }
  
  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.info("Creating Connection Factory");
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  //SPOUTS
  
  @Qualifier("realtime")
  @Bean(destroyMethod = "close")
  public JmsSpout buildJmsSpout(ConnectionFactory factory) throws Exception {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(new DestinationProvider(factory, Destinations.TOPIC_REALTIME_MARKETDATA));
    spout.setJmsTupleProducer(new MarketDataTupleProducer());
    spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout.setDistributed(true);
    spout.setRecoveryPeriod(1000);

    return spout;

  }

  @Qualifier("retroModelData")
  @Bean(destroyMethod = "close")
  public JmsSpout buildJRetroModelJmsSpout(ConnectionFactory factory) throws Exception {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(new DestinationProvider(factory, Destinations.QUEUE_RETRO_MODEL_DATA));
    spout.setJmsTupleProducer(new MarketDataTupleProducer());
    spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout.setDistributed(true);
    spout.setRecoveryPeriod(1000);

    return spout;

  }
  
  @Qualifier("commands")
  @Bean(destroyMethod = "close")
  public JmsSpout buildJmsCommandsSpout(ConnectionFactory factory) throws Exception {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(new DestinationProvider(factory, Destinations.TOPIC_COMMANDS));
    spout.setJmsTupleProducer(new CommandsTupleProducer());
    spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout.setDistributed(true);
    spout.setRecoveryPeriod(1000);

    return spout;

  }

  
  //BOLTS
  
  @Qualifier("notification")
  @Bean
  public JmsBolt buildNotificationJmsBolt(ConnectionFactory factory) throws Exception {

    JmsBolt bolt = new JmsBolt();
    bolt.setAutoAck(false);
    bolt.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    bolt.setJmsMessageProducer(new NotificationMessageProducer());
    bolt.setJmsProvider(new DestinationProvider(factory, Destinations.TOPIC_NOTIFICATIONS));
    return bolt;
  }

  @Qualifier("modelData")
  @Bean
  public JmsBolt buildModelDataJmsBolt(ConnectionFactory factory) throws Exception {

    JmsBolt bolt = new JmsBolt();
    bolt.setAutoAck(false);
    bolt.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    bolt.setJmsMessageProducer(new ModelDataMessageProducer());
    bolt.setJmsProvider(new DestinationProvider(factory, Destinations.QUEUE_MODEL_DATA));
    return bolt;
  }

  @Bean
  public Clock backtestClock() {
    return new BacktestClock();
  }

  @Bean
  public FieldTypeSplittingBolt buildFiledTypeSplittingBolt() {
    return new FieldTypeSplittingBolt();
  }


}
