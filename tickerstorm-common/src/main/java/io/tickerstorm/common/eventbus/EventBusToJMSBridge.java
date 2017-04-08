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
import javax.annotation.PreDestroy;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventBusToJMSBridge {

  private static final Logger logger = LoggerFactory.getLogger(EventBusToJMSBridge.class);
  private final EventBus bus;
  private final ConnectionFactory factory;
  private final Session session;
  private final Connection connection;
  private final MessageProducer producer;
  private final String destination;
  private final String source;
  private long expiration = 0;

  public EventBusToJMSBridge(EventBus eventBus, String destination, ConnectionFactory factory, String source) throws Exception {

    this.bus = eventBus;
    this.destination = destination;
    this.factory = factory;
    this.source = source;

    this.bus.register(this);

    connection = factory.createConnection();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = ByDestinationNameJmsResolver.resolveDestinationName(session, destination);
    producer = session.createProducer(dest);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    connection.start();

  }

  public EventBusToJMSBridge(EventBus eventBus, String destination, ConnectionFactory factory, String source, long expiration) throws Exception {
    this(eventBus, destination, factory, source);
    this.expiration = expiration;
  }

  @PreDestroy
  public void destroy() throws Exception {
    if (bus != null) {
      bus.unregister(this);
    }

    producer.close();
    session.close();
    connection.close();
  }

  @Subscribe
  public void onEvent(Serializable data) {

    try {

      Message m = session.createObjectMessage(data);
      m.setStringProperty("source", source);

      if (expiration > 0) {
        m.setJMSExpiration(expiration);
      }

      producer.send(m);

      logger.trace(source + " sent " + ((ObjectMessage) m).getObject().toString() + " to destination " + destination);

    } catch (JMSException e) {
      Throwable ex = com.google.common.base.Throwables.getRootCause(e);
      logger.error(ex.getMessage(), ex);
    }
  }

}
