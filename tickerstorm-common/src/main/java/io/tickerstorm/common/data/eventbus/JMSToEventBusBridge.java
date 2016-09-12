package io.tickerstorm.common.data.eventbus;

import java.io.Serializable;

import javax.jms.ObjectMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;

public class JMSToEventBusBridge {

  public static final Logger logger = LoggerFactory.getLogger(JMSToEventBusBridge.class);

  public EventBus queryBus;

  public EventBus realtimeBus;

  public EventBus commandsBus;

  public EventBus notificationBus;

  public EventBus modelDataBus;

  public EventBus retroModelDataBus;

  @JmsListener(destination = Destinations.TOPIC_COMMANDS)
  public void onCommandMessage(ObjectMessage md) throws Exception {
    if (commandsBus != null) {
      logger.trace("Received command " + md.toString());
      commandsBus.post(md.getObject());
    }
  }

  @JmsListener(destination = Destinations.TOPIC_REALTIME_MARKETDATA)
  public void onMessage(@Payload MarketData md) {
    if (realtimeBus != null) {
      logger.trace("Received market data " + md.toString());
      realtimeBus.post(md);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_MODEL_DATA)
  public void onMessage(ObjectMessage md) throws Exception {       
    if (modelDataBus != null) {
      logger.trace("Received model data " + md.getObject().toString());
      modelDataBus.post(md.getObject());
    }
  }

  @JmsListener(destination = Destinations.QUEUE_RETRO_MODEL_DATA)
  public void onRetroMessage(@Payload Serializable row) {
    if (retroModelDataBus != null) {
      logger.trace("Received retro model data " + row.toString());
      retroModelDataBus.post(row);
    }
  }

  public void setRetroModelDataBus(EventBus retroModelDataBus) {
    this.retroModelDataBus = retroModelDataBus;
  }

  @JmsListener(destination = Destinations.TOPIC_NOTIFICATIONS)
  public void onNotificationMessage(@Payload Object md) {
    if (notificationBus != null) {

      logger.trace("Received notification " + md.toString());
      notificationBus.post(md);

    }
  }


}
