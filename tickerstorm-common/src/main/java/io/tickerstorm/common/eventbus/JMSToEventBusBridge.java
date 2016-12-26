package io.tickerstorm.common.eventbus;

import javax.jms.ObjectMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListenerConfigurer;
import org.springframework.jms.config.JmsListenerEndpointRegistrar;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;

import com.google.common.eventbus.EventBus;

public class JMSToEventBusBridge implements JmsListenerConfigurer {

  public static final Logger logger = LoggerFactory.getLogger(JMSToEventBusBridge.class);

  public EventBus realtimeBus;

  public EventBus commandsBus;

  public EventBus notificationBus;

  public EventBus modelDataBus;

  public EventBus retroModelDataBus;

  public EventBus brokerFeedBus;

  private String consumer;

  public JMSToEventBusBridge(String consumer) {
    this.consumer = consumer;
  }

  private void register(JmsListenerEndpointRegistrar registrar, String consumer, EventBus bus, String destination, String concurrency) {
    if (bus != null) {
      SimpleJmsListenerEndpoint endpoint = new SimpleJmsListenerEndpoint();
      endpoint.setConcurrency(concurrency);
      endpoint.setDestination(destination);
      endpoint.setId(consumer + "-" + destination);
      // endpoint.setSelector("source IS NULL OR source = '" + consumer + "'");
      endpoint.setMessageListener(message -> {

        String source = null;
        Object o = null;

        try {
          source = message.getStringProperty("source");
          o = ((ObjectMessage) message).getObject();

          logger.trace(consumer + " received " + o.toString() + " from source " + source + ", " + destination);

          bus.post(o);
          message.acknowledge();

        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }
      });


      registrar.registerEndpoint(endpoint);

    }
  }

  public void subsribeToModelDataBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, modelDataBus, Destinations.QUEUE_MODEL_DATA, "1-2");
  }

  public void subsribeToCommandBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, commandsBus, "Consumer." + consumer + "." + Destinations.TOPIC_COMMANDS, "1-2");
  }

  public void subsribeToBrokerFeedBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, brokerFeedBus, Destinations.QUEUE_REALTIME_BROKERFEED, "1-2");
  }

  public void subsribeToRealtimeBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, realtimeBus, "Consumer." + consumer + "." + Destinations.TOPIC_REALTIME_MARKETDATA, "1-4");
  }

  public void subsribeToRetroModelDataBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, retroModelDataBus, Destinations.QUEUE_RETRO_MODEL_DATA, "1-2");
  }

  public void subsribeToNotificationsBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, notificationBus, "Consumer." + consumer + "." + Destinations.TOPIC_NOTIFICATIONS, "1-2");
  }

  @Override
  public void configureJmsListeners(JmsListenerEndpointRegistrar registrar) {

    if (modelDataBus != null)
      subsribeToModelDataBus(registrar, consumer);

    if (commandsBus != null)
      subsribeToCommandBus(registrar, consumer);

    if (brokerFeedBus != null)
      subsribeToBrokerFeedBus(registrar, consumer);

    if (realtimeBus != null)
      subsribeToRealtimeBus(registrar, consumer);

    if (notificationBus != null)
      subsribeToNotificationsBus(registrar, consumer);

    if (retroModelDataBus != null)
      subsribeToRetroModelDataBus(registrar, consumer);
  }
}
