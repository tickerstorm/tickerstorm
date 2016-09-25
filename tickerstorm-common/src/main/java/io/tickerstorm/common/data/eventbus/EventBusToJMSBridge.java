package io.tickerstorm.common.data.eventbus;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class EventBusToJMSBridge {

  private static final Logger logger = LoggerFactory.getLogger(EventBusToJMSBridge.class);

  public EventBusToJMSBridge(EventBus eventBus, String destination, JmsTemplate template, String source) {
    this.bus = eventBus;
    this.destination = destination;
    this.template = template;
    this.source = source;
    this.expiration = 0;

    if (destination.contains("topic"))
      template.setPubSubDomain(true);
  }

  public EventBusToJMSBridge(EventBus eventBus, String destination, JmsTemplate template, String source, long expiration) {
    this.bus = eventBus;
    this.destination = destination;
    this.template = template;
    this.source = source;
    this.expiration = expiration;

    if (destination.contains("topic"))
      template.setPubSubDomain(true);
  }

  private final EventBus bus;
  private final JmsTemplate template;
  private final String destination;
  private final String source;
  private final long expiration;

  @PostConstruct
  public void init() {
    if (bus != null)
      bus.register(this);
  }

  @PreDestroy
  public void destroy() {
    if (bus != null)
      bus.unregister(this);
  }

  @Subscribe
  public void onEvent(Serializable data) {

    template.send(destination, new MessageCreator() {

      @Override
      public Message createMessage(Session session) throws JMSException {
        logger.trace(
            "Dispatching " + data.toString() + " to destination " + destination + " from service " + source + ", " + bus.identifier());
        Message m = session.createObjectMessage(data);
        m.setStringProperty("source", source);
        
        if(expiration > 0)
          m.setJMSExpiration(expiration);
        
        return m;
      }
    });
  }

}
