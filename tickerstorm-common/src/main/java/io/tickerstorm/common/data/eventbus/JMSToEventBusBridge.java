package io.tickerstorm.common.data.eventbus;

import java.io.Serializable;

import javax.jms.ObjectMessage;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.entity.MarketData;

public class JMSToEventBusBridge {

  public static final Logger logger = LoggerFactory.getLogger(JMSToEventBusBridge.class);

  public EventBus queryBus;

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

  @JmsListener(destination = Destinations.TOPIC_COMMANDS)
  public void onCommandMessage(ObjectMessage md) throws Exception {
    final String source = md.getStringProperty("source");
    final Object o = md.getObject();
    if (commandsBus != null
        && (StringUtils.isEmpty(md.getStringProperty("source")) || !md.getStringProperty("source").equalsIgnoreCase(consumer))) {
      logger.trace(consumer + " received command " + o.toString() + " from " + source);
      commandsBus.post(o);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_REALTIME_BROKERFEED)
  public void onBrokerFeedMessage(@Payload MarketData md, @Header("source") String source) {
    if (realtimeBus != null && (StringUtils.isEmpty(source) || !source.equalsIgnoreCase(consumer))) {
      logger.debug(consumer + " received market data " + md.toString());
      brokerFeedBus.post(md);
    }
  }

  @JmsListener(destination = Destinations.TOPIC_REALTIME_MARKETDATA)
  public void onMessage(@Payload MarketData md, @Header("source") String source) {
    if (realtimeBus != null && (StringUtils.isEmpty(source) || !source.equalsIgnoreCase(consumer))) {
      logger.debug(consumer + " received market data " + md.toString());
      realtimeBus.post(md);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_MODEL_DATA)
  public void onMessage(ObjectMessage md, @Header("source") String source) throws Exception {
    final Object o = md.getObject();
    if (modelDataBus != null && (StringUtils.isEmpty(source) || !source.equalsIgnoreCase(consumer))) {
      logger.trace(consumer + " received model data " + o.toString());
      modelDataBus.post(o);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_RETRO_MODEL_DATA)
  public void onRetroMessage(@Payload Serializable row, @Header("source") String source) {
    if (retroModelDataBus != null && (StringUtils.isEmpty(source) || !source.equalsIgnoreCase(consumer))) {
      logger.trace(consumer + " received retro model data " + row.toString());
      retroModelDataBus.post(row);
    }
  }

  public void setRetroModelDataBus(EventBus retroModelDataBus) {
    this.retroModelDataBus = retroModelDataBus;
  }

  @JmsListener(destination = Destinations.TOPIC_NOTIFICATIONS)
  public void onNotificationMessage(@Payload Object md, @Header("source") String source) {
    if (notificationBus != null && (StringUtils.isEmpty(source) || !source.equalsIgnoreCase(consumer))) {
      logger.trace(consumer + " received notification " + md.toString());
      notificationBus.post(md);

    }
  }


}
