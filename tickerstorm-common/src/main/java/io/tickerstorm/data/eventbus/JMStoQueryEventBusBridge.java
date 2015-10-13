package io.tickerstorm.data.eventbus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import net.engio.mbassy.bus.MBassador;

public class JMStoQueryEventBusBridge {

  @Qualifier("query")
  @Autowired
  private MBassador<HistoricalFeedQuery> queryBus;

  @JmsListener(destination = Destinations.QUEUE_QUERY)
  public void onMessage(@Payload HistoricalFeedQuery query) {
    queryBus.publish(query);
  }

}
