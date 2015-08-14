package io.tickerstorm.data.messaging;

import io.tickerstorm.entity.MarketData;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

@Qualifier("realtime")
@Component
public class EventBusToJMSBridge {

  @Qualifier("realtime")
  @Autowired
  private EventBus realtimeBus;

  @Qualifier("realtime")
  @Autowired
  private JmsTemplate relatimeTemplate;

  @PostConstruct
  public void init() {
    realtimeBus.register(this);
  }

  @Subscribe
  public void onMarketData(MarketData data) {

    relatimeTemplate.send(new MessageCreator() {
      @Override
      public Message createMessage(Session session) throws JMSException {
        Message m = session.createObjectMessage(data);
        m.setJMSExpiration(2000);
        return m;
      }
    });
  }


}
