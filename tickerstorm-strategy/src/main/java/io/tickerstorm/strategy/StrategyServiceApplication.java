package io.tickerstorm.strategy;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JmsToEventBusBridge;
import io.tickerstorm.service.HeartBeatGenerator;
import java.util.concurrent.Executor;
import javax.jms.ConnectionFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@SpringBootApplication(
    scanBasePackages = {"io.tickerstorm.strategy.backtest", "io.tickerstorm.strategy.processor", "io.tickerstorm.strategy.util"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class StrategyServiceApplication {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(StrategyServiceApplication.class);
  @Value("${service.name:strategy-service}")
  private String SERVICE;
  @Autowired
  private GaugeService gaugeService;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(StrategyServiceApplication.class, args);
  }

  @Bean
  public static PropertySourcesPlaceholderConfigurer buildConfig() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  @Bean
  public HeartBeatGenerator generateStrategyServiceHeartBeat() {
    return new HeartBeatGenerator(SERVICE, 5000);
  }

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Bean
  public EventBus buildModelDataEventBus() {
    return new EventBus(Destinations.MODEL_DATA_BUS);
  }

  // SENDERS
  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Bean
  public EventBusToJMSBridge buildModelDataJmsBridge(@Qualifier(Destinations.MODEL_DATA_BUS) EventBus eventbus, ConnectionFactory template)
      throws Exception {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_MODEL_DATA, template, SERVICE);
  }

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge2(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus eventbus,
      ConnectionFactory template) throws Exception {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template, SERVICE, 5000);
  }

  // RECEIVERS
  @Bean
  public JmsToEventBusBridge buildRealtimeDataJmsBridge(@Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus modelDataBus,
      ConnectionFactory factory) throws Exception {
    return new JmsToEventBusBridge(factory, modelDataBus, Destinations.TOPIC_REALTIME_MARKETDATA);
  }

  @Bean
  public JmsToEventBusBridge buildCommandDataJmsBridge(@Qualifier(Destinations.COMMANDS_BUS) EventBus modelDataBus, ConnectionFactory factory)
      throws Exception {
    return new JmsToEventBusBridge(factory, modelDataBus, Destinations.TOPIC_COMMANDS);
  }

  @Bean
  public JmsToEventBusBridge buildRetroModelDataJmsBridge(@Qualifier(Destinations.RETRO_MODEL_DATA_BUS) EventBus modelDataBus,
      ConnectionFactory factory) throws Exception {
    return new JmsToEventBusBridge(factory, modelDataBus, Destinations.QUEUE_RETRO_MODEL_DATA);
  }

  @Qualifier("processorEventBus")
  @Bean
  public EventBus buildEventProcessorBus() {
    return new EventBus("processorEventBus");
  }


}
