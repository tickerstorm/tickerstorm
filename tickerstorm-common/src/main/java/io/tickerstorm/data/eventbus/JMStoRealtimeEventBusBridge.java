package io.tickerstorm.data.eventbus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;

import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

public class JMStoRealtimeEventBusBridge {

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> bus;

  @JmsListener(destination = Destinations.TOPIC_REALTIME_MARKETDATA)
  public void onMessage(@Payload MarketData md) {
    bus.publish(md);
  }

}
