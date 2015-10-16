package io.tickerstorm.common.data.eventbus;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;

import io.tickerstorm.common.data.feed.HistoricalFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

public class JMSToEventBusBridge {

  public static final Logger logger = LoggerFactory.getLogger(JMSToEventBusBridge.class);

  private MBassador<HistoricalFeedQuery> queryBus;

  private MBassador<MarketData> realtimeBus;

  private MBassador<Serializable> commandsBus;

  private MBassador<Serializable> notificationBus;

  public MBassador<Serializable> getCommandsBus() {
    return commandsBus;
  }

  public MBassador<Serializable> getNotificationBus() {
    return notificationBus;
  }

  public MBassador<HistoricalFeedQuery> getQueryBus() {
    return queryBus;
  }

  public MBassador<MarketData> getRealtimeBus() {
    return realtimeBus;
  }

  @JmsListener(destination = Destinations.TOPIC_COMMANDS)
  public void onCommandMessage(@Payload Serializable md) {
    if (commandsBus != null) {
      logger.debug("Received command " + md.toString());
      commandsBus.publishAsync(md);
    }
  }

  @JmsListener(destination = Destinations.QUEUE_QUERY)
  public void onMessage(@Payload HistoricalFeedQuery query) {
    if (queryBus != null) {
      logger.debug("Received query " + query.toString());
      queryBus.publishAsync(query);
    }
  }

  @JmsListener(destination = Destinations.TOPIC_REALTIME_MARKETDATA)
  public void onMessage(@Payload MarketData md) {
    if (realtimeBus != null) {
      logger.debug("Received market data " + md.toString());
      realtimeBus.publishAsync(md);
    }
  }

  @JmsListener(destination = Destinations.TOPIC_NOTIFICATIONS)
  public void onNotificationMessage(@Payload Serializable md) {
    if (notificationBus != null) {
      logger.debug("Received notification " + md.toString());
      notificationBus.publishAsync(md);
    }
  }

  public void setCommandsBus(MBassador<Serializable> commandsBus) {
    this.commandsBus = commandsBus;
  }

  public void setNotificationBus(MBassador<Serializable> notificationBus) {
    this.notificationBus = notificationBus;
  }

  public void setQueryBus(MBassador<HistoricalFeedQuery> queryBus) {
    this.queryBus = queryBus;
  }

  public void setRealtimeBus(MBassador<MarketData> realtimeBus) {
    this.realtimeBus = realtimeBus;
  }

}
