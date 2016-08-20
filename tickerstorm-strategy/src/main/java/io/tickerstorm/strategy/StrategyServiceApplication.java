package io.tickerstorm.strategy;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Executors;

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

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

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
  public EventBusToJMSBridge buildModelDataJmsBridge(@Qualifier(Destinations.MODEL_DATA_BUS) AsyncEventBus eventbus,
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
    return new AsyncEventBus(Executors.newFixedThreadPool(2), new AsyncEventBusExceptionHandler());
  }

  @Qualifier("retroEventBus")
  @Bean
  public AsyncEventBus buildRetroEventProcessorBus() {
    return new AsyncEventBus(Executors.newFixedThreadPool(2), new AsyncEventBusExceptionHandler());
  }

  @Bean
  public Clock backtestClock() {
    return new BacktestClock();
  }

  private class AsyncEventBusExceptionHandler implements SubscriberExceptionHandler {

    @Override
    public void handleException(Throwable exception, SubscriberExceptionContext context) {
      logger.error("Event " + context.getEvent() + " threw an exception on listener " + context.getSubscriber() + " with exception "
          + Throwables.getRootCause(exception));

    }

  }
}
