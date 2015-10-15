package io.tickerstorm.common.data.eventbus;

import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;

import io.tickerstorm.common.data.feed.HistoricalFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

public class JMSToEventBusBridge {

  private MBassador<HistoricalFeedQuery> queryBus;

  private MBassador<MarketData> realtimeBus;

  @JmsListener(destination = Destinations.TOPIC_REALTIME_MARKETDATA)
  public void onMessage(@Payload MarketData md) {
    if (realtimeBus != null)
      realtimeBus.publish(md);
  }

  public MBassador<HistoricalFeedQuery> getQueryBus() {
    return queryBus;
  }

  public void setQueryBus(MBassador<HistoricalFeedQuery> queryBus) {
    this.queryBus = queryBus;
  }

  public MBassador<MarketData> getRealtimeBus() {
    return realtimeBus;
  }

  public void setRealtimeBus(MBassador<MarketData> realtimeBus) {
    this.realtimeBus = realtimeBus;
  }

  @JmsListener(destination = Destinations.QUEUE_QUERY)
  public void onMessage(@Payload HistoricalFeedQuery query) {
    if (queryBus != null)
      queryBus.publish(query);
  }

}
