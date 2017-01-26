/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.common.eventbus;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import java.io.Serializable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DestinationResolver;

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

    try {

      CachingConnectionFactory factory = (CachingConnectionFactory) template.getConnectionFactory();
      Session session = factory.createConnection()
          .createSession(false, template.getSessionAcknowledgeMode());
      DestinationResolver resolver = template.getDestinationResolver();

      MessageProducer producer = session
          .createProducer(resolver.resolveDestinationName(session, destination, true));
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      Message m = session.createObjectMessage(data);
      m.setStringProperty("source", source);

      if (expiration > 0) {
        m.setJMSExpiration(expiration);
      }

      producer.send(m);

    } catch (JMSException e) {
      Throwable ex = com.google.common.base.Throwables.getRootCause(e);
      logger.error(ex.getMessage(), ex);
    }

    // template.send(destination, new MessageCreator() {
    //
    // @Override
    // public Message createMessage(Session session) throws JMSException {
    // logger.trace(
    // "Dispatching " + data.toString() + " to destination " + destination + " from service " +
    // source + ", " + bus.identifier());
    // Message m = session.createObjectMessage(data);
    // m.setStringProperty("source", source);
    //
    // if (expiration > 0)
    // m.setJMSExpiration(expiration);
    //
    // return m;
    // }
    // });
  }

}
