package io.tickerstorm.strategy;

import org.apache.logging.log4j.core.util.Throwables;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;

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
  public EventBusToJMSBridge buildModelDataJmsBridge(@Qualifier(Destinations.MODEL_DATA_BUS) EventBus eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_MODEL_DATA, template);
  }

  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template);
  }

  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus realtimeBus,
      @Qualifier(Destinations.COMMANDS_BUS) EventBus commandsBus,
      @Qualifier(Destinations.RETRO_MODEL_DATA_BUS) EventBus retroModelDataBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.realtimeBus = realtimeBus;
    bridge.commandsBus = commandsBus;
    bridge.retroModelDataBus = retroModelDataBus;
    return bridge;
  }

  @Qualifier("processorEventBus")
  @Bean
  public EventBus buildEventProcessorBus() {
    return new EventBus(new AsyncEventBusExceptionHandler());
  }

  private class AsyncEventBusExceptionHandler implements SubscriberExceptionHandler {

    @Override
    public void handleException(Throwable exception, SubscriberExceptionContext context) {
      logger.error("Event " + context.getEvent() + " threw an exception on listener " + context.getSubscriber() + " with exception "
          + Throwables.getRootCause(exception));

    }

  }
}
