package io.tickerstorm.messaging;

import io.tickerstorm.entity.MarketData;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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

  @Qualifier("historical")
  @Autowired
  private EventBus historicalBus;

  @Qualifier("historical")
  @Autowired
  private MessageProducer producer;

  @Autowired
  private Session session;
  
  @PostConstruct
  public void init(){
    historicalBus.register(this);
  }

  @PreDestroy
  public void destroy(){
    historicalBus.unregister(this);
  }
  
  @Subscribe
  public void onMarketData(MarketData data) {

    try {
      
      Message m = session.createObjectMessage();
      m.setObjectProperty("marketdata", data);
      m.setJMSExpiration(2000);
      producer.send(m);
      
    } catch (JMSException e) {
      logger.error(e.getMessage(), Throwables.getRootCause(e));
    }

  }

}
