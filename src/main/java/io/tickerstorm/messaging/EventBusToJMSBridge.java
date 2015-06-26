package io.tickerstorm.messaging;

import io.tickerstorm.entity.MarketData;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

@Component
public class EventBusToJMSBridge {

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(EventBusToJMSBridge.class);

  // @Qualifier("historical")
  // @Autowired
  // private EventBus historicalBus;

  @Qualifier("realtime")
  @Autowired
  private EventBus realtimeBus;

  @Qualifier("realtime")
  @Autowired
  private MessageProducer rtProducer;

  // @Qualifier("historical")
  // @Autowired
  // private MessageProducer histProducer;

  @Autowired
  private Session session;

  @PostConstruct
  public void init() {
    // historicalBus.register(new HistoricalBridge());
    realtimeBus.register(new RealtimeBridge());
  }

  // private class HistoricalBridge {
  //
  // @Subscribe
  // public void onMarketData(MarketData data) {
  //
  // try {
  //
  // Message m = session.createObjectMessage(data);
  // m.setJMSExpiration(2000);
  // histProducer.send(m);
  //
  // } catch (JMSException e) {
  // logger.error(e.getMessage(), Throwables.getRootCause(e));
  // }
  //
  // }
  // }

  private class RealtimeBridge {

    @Subscribe
    public void onMarketData(MarketData data) {

      try {

        Message m = session.createObjectMessage(data);
        m.setJMSExpiration(2000);
        rtProducer.send(m);

      } catch (JMSException e) {
        logger.error(e.getMessage(), Throwables.getRootCause(e));
      }

    }
  }

}
