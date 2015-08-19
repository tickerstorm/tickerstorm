package io.tickerstorm.data.messaging;

import io.tickerstorm.entity.MarketData;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

@Qualifier("realtime")
@Component
@Listener(references = References.Strong)
public class EventBusToJMSBridge {

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  @Autowired
  private JmsTemplate relatimeTemplate;

  @PostConstruct
  public void init() {
    realtimeBus.subscribe(this);
  }

  @Handler
  public void onMarketData(MarketData data) {

    relatimeTemplate.send(Destinations.TOPIC_REALTIME_MARKETDATA, new MessageCreator() {
      @Override
      public Message createMessage(Session session) throws JMSException {
        Message m = session.createObjectMessage(data);
        return m;
      }
    });
  }


}
