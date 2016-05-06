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

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.IMessageFilter;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@Listener(references = References.Strong)
public class EventBusToJMSBridge {

  private static final Logger logger = LoggerFactory.getLogger(EventBusToJMSBridge.class);

  public EventBusToJMSBridge(MBassador<?> eventBus, String destination, JmsTemplate template) {
    this.bus = eventBus;
    this.destination = destination;
    this.template = template;

    if (destination.contains("topic"))
      template.setPubSubDomain(true);
  }

  public EventBusToJMSBridge(MBassador<?> eventBus, String destination, JmsTemplate template, IMessageFilter<Serializable> filter) {
    this(eventBus, destination, template);
    this.filter = filter;
  }

  private MBassador<?> bus;
  private JmsTemplate template;
  private String destination;
  private IMessageFilter<Serializable> filter = new BaseFilter();

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

    if (filter != null && filter.accepts(data, null)) {

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



}
