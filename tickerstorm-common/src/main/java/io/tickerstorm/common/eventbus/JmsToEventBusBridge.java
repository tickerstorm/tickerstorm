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
import javax.annotation.PreDestroy;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.RedeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kkarski on 4/6/17.
 */
public class JmsToEventBusBridge {

  private static final Logger logger = LoggerFactory.getLogger(JmsToEventBusBridge.class);
  private final MessageConsumer consumer;
  private final Session session;
  private final ActiveMQConnection connection;

  public JmsToEventBusBridge(ConnectionFactory connectionFactory, final EventBus eventBus, String destination) throws Exception {
    this(connectionFactory, eventBus, destination, -1, -1);
  }

  public JmsToEventBusBridge(ConnectionFactory connectionFactory, final EventBus eventBus, String destination, int prefetch, int redeliveryDelay)
      throws Exception {

    connection = (ActiveMQConnection) (ActiveMQConnection) connectionFactory.createConnection();

    if (prefetch > 0) {
      ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
      policy.setQueuePrefetch(prefetch);
      policy.setTopicPrefetch(prefetch);
      connection.setPrefetchPolicy(policy);
    }

    if (redeliveryDelay > 0) {
      RedeliveryPolicy policy = new RedeliveryPolicy();
      policy.setInitialRedeliveryDelay(redeliveryDelay);
      policy.setRedeliveryDelay(redeliveryDelay);
      connection.setRedeliveryPolicy(policy);
    }

    session = connection.createSession(false, ActiveMQSession.CLIENT_ACKNOWLEDGE);
    Destination dest = ByDestinationNameJmsResolver.resolveDestinationName(session, destination);

    consumer = ((ActiveMQSession) session).createConsumer(dest, new MessageListener() {
      @Override
      public void onMessage(Message m) {

        try {
          String source = m.getStringProperty("source");

          if (m instanceof ObjectMessage) {

            logger.trace(connection.getClientID() + " received " + ((ObjectMessage) m).getObject().toString() + " from source " + source + ", " + destination);

            eventBus.post(((ObjectMessage) m).getObject());
            m.acknowledge();

          } else {
            logger.error(
                connection.getClientID() + " received " + m.toString() + " from source " + source + ", " + destination + ". Not acceptanle message type.");
          }

        } catch (JMSException e) {
          logger.error(e.getMessage());

        }

      }
    });
    connection.start();

  }

  @PreDestroy
  public void shutdown() throws Exception {
    consumer.close();
    session.close();
    connection.close();
  }
}
