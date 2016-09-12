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

  public EventBusToJMSBridge(EventBus eventBus, String destination, JmsTemplate template) {
    this.bus = eventBus;
    this.destination = destination;
    this.template = template;

    if (destination.contains("topic"))
      template.setPubSubDomain(true);
  }

  private EventBus asyncBus;
  private EventBus bus;
  private JmsTemplate template;
  private String destination;


  @PostConstruct
  public void init() {
    if (bus != null)
      bus.register(this);

    if (asyncBus != null)
      asyncBus.register(this);
  }

  @PreDestroy
  public void destroy() {
    if (asyncBus != null)
      asyncBus.register(this);

    if (bus != null)
      bus.register(this);
  }

  @Subscribe
  public void onEvent(Serializable data) {



    template.send(destination, new MessageCreator() {

      @Override
      public Message createMessage(Session session) throws JMSException {
        logger.trace("Dispatching " + data.toString() + " to destination " + destination);
        Message m = session.createObjectMessage(data);
        return m;
      }
    });

  }



}
