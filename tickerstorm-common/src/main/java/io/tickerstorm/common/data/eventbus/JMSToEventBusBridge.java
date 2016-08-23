package io.tickerstorm.common.data.eventbus;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;

import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

public class JMSToEventBusBridge {

  public static final Logger logger = LoggerFactory.getLogger(JMSToEventBusBridge.class);

  private MBassador<DataFeedQuery> queryBus;

  private MBassador<MarketData> realtimeBus;

  private MBassador<Serializable> commandsBus;

  private MBassador<Serializable> notificationBus;

  private AsyncEventBus modelDataBus;

  private AsyncEventBus retroModelDataBus;

  public AsyncEventBus getModelDataBus() {
    return modelDataBus;
  }

  public void setModelDataBus(AsyncEventBus modelDataBus) {
    this.modelDataBus = modelDataBus;
  }

  private boolean explodeCollections = false;

  public boolean isExplodeCollections() {
    return explodeCollections;
  }

  public void setExplodeCollections(boolean explodeCollections) {
    this.explodeCollections = explodeCollections;
  }

  public MBassador<Serializable> getCommandsBus() {
    return commandsBus;
  }

  public MBassador<Serializable> getNotificationBus() {
    return notificationBus;
  }

  public MBassador<DataFeedQuery> getQueryBus() {
    return queryBus;
  }

  public MBassador<MarketData> getRealtimeBus() {
    return realtimeBus;
  }

  @JmsListener(destination = Destinations.TOPIC_COMMANDS)
  public void onCommandMessage(@Payload Serializable md) {
    if (commandsBus != null) {

      if (Collection.class.isAssignableFrom(md.getClass()) && explodeCollections) {

        for (Serializable s : (Collection<Serializable>) md) {
          logger.trace("Received command  " + md.toString());
          commandsBus.publishAsync(s);
        }

      } else {

        logger.trace("Received command " + md.toString());
        commandsBus.publishAsync(md);

      }
    }
  }

  @JmsListener(destination = Destinations.QUEUE_HISTORICAL_DATA_QUERY)
  public void onMessage(@Payload DataFeedQuery query) {
    if (queryBus != null) {
      logger.trace("Received feed query " + query.toString());
      queryBus.publishAsync(query);
    }
  }

  @JmsListener(destination = Destinations.TOPIC_REALTIME_MARKETDATA)
  public void onMessage(@Payload MarketData md) {
    if (realtimeBus != null) {
      logger.trace("Received market data " + md.toString());
      realtimeBus.publishAsync(md);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_MODEL_DATA)
  public void onMessage(@Payload Serializable field) {
    if (modelDataBus != null) {
      logger.trace("Received model data " + field.toString());
      modelDataBus.post(field);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_RETRO_MODEL_DATA)
  public void onRetroMessage(@Payload Serializable row) {
    if (retroModelDataBus != null) {
      logger.trace("Received retro model data " + row.toString());
      retroModelDataBus.post(row);
    }
  }

  public AsyncEventBus getRetroModelDataBus() {
    return retroModelDataBus;
  }

  public void setRetroModelDataBus(AsyncEventBus retroModelDataBus) {
    this.retroModelDataBus = retroModelDataBus;
  }

  @JmsListener(destination = Destinations.TOPIC_NOTIFICATIONS)
  public void onNotificationMessage(@Payload Serializable md) {
    if (notificationBus != null) {

      if (Collection.class.isAssignableFrom(md.getClass()) && explodeCollections) {

        for (Serializable s : (Collection<Serializable>) md) {
          logger.trace("Received notification " + md.toString());
          notificationBus.publishAsync(s);
        }

      } else {

        logger.trace("Received notification " + md.toString());
        notificationBus.publishAsync(md);
      }
    }
  }

  public void setCommandsBus(MBassador<Serializable> commandsBus) {
    this.commandsBus = commandsBus;
  }

  public void setNotificationBus(MBassador<Serializable> notificationBus) {
    this.notificationBus = notificationBus;
  }

  public void setQueryBus(MBassador<DataFeedQuery> queryBus) {
    this.queryBus = queryBus;
  }

  public void setRealtimeBus(MBassador<MarketData> realtimeBus) {
    this.realtimeBus = realtimeBus;
  }

}
