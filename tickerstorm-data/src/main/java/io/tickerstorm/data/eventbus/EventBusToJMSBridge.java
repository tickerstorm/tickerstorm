package io.tickerstorm.data.eventbus;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@Listener(references = References.Strong)
public class EventBusToJMSBridge {

  private static final Logger logger = LoggerFactory.getLogger(EventBusToJMSBridge.class);
  private AtomicLong count = new AtomicLong(0);

  public EventBusToJMSBridge(MBassador<?> eventBus, String destination, JmsTemplate template) {
    this.bus = eventBus;
    this.destination = destination;
    this.template = template;
  }

  private MBassador<?> bus;
  private JmsTemplate template;
  private String destination;

  @PostConstruct
  public void init() {
    bus.subscribe(this);
  }

  @PreDestroy
  public void destroy() {
    bus.unsubscribe(this);
  }

  @Handler
  public void onEvent(Serializable data) {

    logger.debug("Event bridge to " + destination + " dispatched a total of "
        + count.incrementAndGet() + " events");

    template.send(destination, new MessageCreator() {
      @Override
      public Message createMessage(Session session) throws JMSException {
        Message m = session.createObjectMessage(data);
        return m;
      }
    });
  }



}
