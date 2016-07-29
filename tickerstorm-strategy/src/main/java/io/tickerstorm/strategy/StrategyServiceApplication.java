package io.tickerstorm.strategy;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.util.BacktestClock;
import io.tickerstorm.strategy.util.Clock;
import net.engio.mbassy.bus.MBassador;

@EnableJms
@SpringBootApplication
@ComponentScan(basePackages = {"io.tickerstorm.strategy.backtest", "io.tickerstorm.strategy.processor", "io.tickerstorm.strategy.util"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class StrategyServiceApplication {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(StrategyServiceApplication.class);

  public static void main(String[] args) throws Exception {
    SpringApplication.run(StrategyServiceApplication.class, args);
  }

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildModelDataJmsBridge(@Qualifier(Destinations.MODEL_DATA_BUS) MBassador<Map<String, Object>> eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_MODEL_DATA, template);
  }

  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) MBassador<Serializable> eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template);
  }

  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier(Destinations.REALTIME_MARKETDATA_BUS) MBassador<MarketData> realtimeBus,
      @Qualifier(Destinations.COMMANDS_BUS) MBassador<Serializable> commandsBus,
      @Qualifier(Destinations.RETRO_MODEL_DATA_BUS) MBassador<Map<String, Object>> retroModelDataBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setRealtimeBus(realtimeBus);
    bridge.setCommandsBus(commandsBus);
    bridge.setRetroModelDataBus(retroModelDataBus);
    return bridge;
  }

  @Qualifier("eventBus")
  @Bean
  public AsyncEventBus buildEventProcessorBus() {
    return new AsyncEventBus("event processor bus", Executors.newFixedThreadPool(2));
  }

  @Qualifier("retroEventBus")
  @Bean
  public AsyncEventBus buildRetroEventProcessorBus() {
    return new AsyncEventBus("retro processor bus", Executors.newFixedThreadPool(2));
  }

  // SPOUTS

  // @Qualifier("realtime")
  // @Bean(destroyMethod = "close")
  // public JmsSpout buildJmsSpout(ConnectionFactory factory) throws Exception {
  //
  // JmsSpout spout = new JmsSpout();
  // spout.setJmsProvider(new DestinationProvider(factory, Destinations.TOPIC_REALTIME_MARKETDATA));
  // spout.setJmsTupleProducer(new MarketDataTupleProducer());
  // spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
  // spout.setDistributed(true);
  // spout.setRecoveryPeriod(1000);
  //
  // return spout;
  //
  // }

  // @Qualifier("retroModelData")
  // @Bean(destroyMethod = "close")
  // public JmsSpout buildJRetroModelJmsSpout(ConnectionFactory factory) throws Exception {
  //
  // JmsSpout spout = new JmsSpout();
  // spout.setJmsProvider(new DestinationProvider(factory, Destinations.QUEUE_RETRO_MODEL_DATA));
  // spout.setJmsTupleProducer(new MarketDataTupleProducer());
  // spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
  // spout.setDistributed(true);
  // spout.setRecoveryPeriod(1000);
  //
  // return spout;
  // }


  // @Qualifier("commands")
  // @Bean(destroyMethod = "close")
  // public JmsSpout buildJmsCommandsSpout(ConnectionFactory factory) throws Exception {
  //
  // JmsSpout spout = new JmsSpout();
  // spout.setJmsProvider(new DestinationProvider(factory, Destinations.TOPIC_COMMANDS));
  // spout.setJmsTupleProducer(new CommandsTupleProducer());
  // spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
  // spout.setDistributed(true);
  // spout.setRecoveryPeriod(1000);
  //
  // return spout;
  //
  // }


  // BOLTS

  // @Qualifier("notification")
  // @Bean
  // public JmsBolt buildNotificationJmsBolt(ConnectionFactory factory) throws Exception {
  //
  // JmsBolt bolt = new JmsBolt();
  // bolt.setAutoAck(false);
  // bolt.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
  // bolt.setJmsMessageProducer(new NotificationMessageProducer());
  // bolt.setJmsProvider(new DestinationProvider(factory, Destinations.TOPIC_NOTIFICATIONS));
  // return bolt;
  // }

  // @Qualifier("modelData")
  // @Bean
  // public JmsBolt buildModelDataJmsBolt(ConnectionFactory factory) throws Exception {
  //
  // JmsBolt bolt = new JmsBolt();
  // bolt.setAutoAck(false);
  // bolt.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
  // bolt.setJmsMessageProducer(new ModelDataMessageProducer());
  // bolt.setJmsProvider(new DestinationProvider(factory, Destinations.QUEUE_MODEL_DATA));
  // return bolt;
  // }

  @Bean
  public Clock backtestClock() {
    return new BacktestClock();
  }
  //
  // @Bean
  // public FieldTypeSplittingBolt buildFiledTypeSplittingBolt() {
  // return new FieldTypeSplittingBolt();
  // }


}
