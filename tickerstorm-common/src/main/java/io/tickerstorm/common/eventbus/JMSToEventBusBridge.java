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
import javax.jms.ObjectMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListenerConfigurer;
import org.springframework.jms.config.JmsListenerEndpointRegistrar;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;

public class JMSToEventBusBridge implements JmsListenerConfigurer {

  public static final Logger logger = LoggerFactory.getLogger(JMSToEventBusBridge.class);

  public EventBus realtimeBus;

  public EventBus commandsBus;

  public EventBus notificationBus;

  public EventBus modelDataBus;

  public EventBus retroModelDataBus;

  public EventBus brokerFeedBus;

  private String consumer;

  public JMSToEventBusBridge(String consumer) {
    this.consumer = consumer;
  }

  private void register(JmsListenerEndpointRegistrar registrar, String consumer, EventBus bus, String destination, String concurrency) {
    if (bus != null) {
      SimpleJmsListenerEndpoint endpoint = new SimpleJmsListenerEndpoint();
      endpoint.setConcurrency(concurrency);
      endpoint.setDestination(destination);
      endpoint.setId(consumer + "-" + destination);
      // endpoint.setSelector("source IS NULL OR source = '" + consumer + "'");
      endpoint.setMessageListener(message -> {

        String source = null;
        Object o = null;

        try {
          source = message.getStringProperty("source");
          o = ((ObjectMessage) message).getObject();

          logger.trace(consumer + " received " + o.toString() + " from source " + source + ", " + destination);

          bus.post(o);
          message.acknowledge();

        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }
      });


      registrar.registerEndpoint(endpoint);

    }
  }

  public void subsribeToModelDataBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, modelDataBus, Destinations.QUEUE_MODEL_DATA, "1");
  }

  public void subsribeToCommandBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, commandsBus,
        "Consumer." + consumer + "." + Destinations.TOPIC_COMMANDS, "1");
  }

  public void subsribeToBrokerFeedBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, brokerFeedBus, Destinations.QUEUE_REALTIME_BROKERFEED, "1");
  }

  public void subsribeToRealtimeBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, realtimeBus,
        "Consumer." + consumer + "." + Destinations.TOPIC_REALTIME_MARKETDATA, "1");
  }

  public void subsribeToRetroModelDataBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, retroModelDataBus, Destinations.QUEUE_RETRO_MODEL_DATA, "1");
  }

  public void subsribeToNotificationsBus(JmsListenerEndpointRegistrar registrar, String consumer) {
    register(registrar, consumer, notificationBus,
        "Consumer." + consumer + "." + Destinations.TOPIC_NOTIFICATIONS, "1");
  }

  @Override
  public void configureJmsListeners(JmsListenerEndpointRegistrar registrar) {

    if (modelDataBus != null)
      subsribeToModelDataBus(registrar, consumer);

    if (commandsBus != null)
      subsribeToCommandBus(registrar, consumer);

    if (brokerFeedBus != null)
      subsribeToBrokerFeedBus(registrar, consumer);

    if (realtimeBus != null)
      subsribeToRealtimeBus(registrar, consumer);

    if (notificationBus != null)
      subsribeToNotificationsBus(registrar, consumer);

    if (retroModelDataBus != null)
      subsribeToRetroModelDataBus(registrar, consumer);
  }
}
