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

package io.tickerstorm.common;

import com.google.common.base.Throwables;
import io.tickerstorm.common.eventbus.ByDestinationNameJmsResolver;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DynamicDestinationResolver;

@EnableJms
@Configuration
@PropertySource({"classpath:/default.properties", "classpath:/application.properties"})
public class JmsEventBusContext {

  private static final Logger logger = LoggerFactory.getLogger(JmsEventBusContext.class);

  @Value("${spring.activemq.broker-url}")
  protected String transport;

  @Value("${spring.jms.listener.acknowledge-mode}")
  protected String acknoledgeMode;

  @Value("${service.name}")
  protected String serviceName;

  @Primary
  @Bean
  public CachingConnectionFactory buildSenderConnectionFactory() {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    connectionFactory.setTrustAllPackages(true);
    connectionFactory.setClientIDPrefix(serviceName);
    connectionFactory.setUseAsyncSend(true);
    connectionFactory.setSendAcksAsync(true);
    connectionFactory.setAlwaysSessionAsync(true);
    connectionFactory.setDispatchAsync(true);
    connectionFactory.setAlwaysSyncSend(false);

    // Not recommended with listener container
    CachingConnectionFactory caching = new CachingConnectionFactory(connectionFactory);
    caching.setSessionCacheSize(10);
    caching.setReconnectOnException(true);
    caching.setExceptionListener(new ExceptionListener() {

      @Override
      public void onException(JMSException exception) {
        logger.error(Throwables.getRootCause(exception).getMessage(), Throwables.getRootCause(exception));

      }
    });
    return caching;
  }

  @Bean
  public PooledConnectionFactory buildReceiverConnectionFactory() {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    connectionFactory.setTrustAllPackages(true);
    connectionFactory.setClientIDPrefix(serviceName);
    connectionFactory.setSendAcksAsync(true);
    connectionFactory.setExceptionListener(new ExceptionListener() {

      @Override
      public void onException(JMSException exception) {
        logger.error(Throwables.getRootCause(exception).getMessage(),
            Throwables.getRootCause(exception));

      }
    });

    // Not recommended with listener container
    PooledConnectionFactory caching = new PooledConnectionFactory();
    caching.setConnectionFactory(connectionFactory);
    caching.setMaxConnections(10);
    caching.setReconnectOnException(true);

    return caching;
  }

  @Bean("jmsListenerContainerFactory")
  public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(
      DefaultJmsListenerContainerFactoryConfigurer configurer,
      PooledConnectionFactory connectionFactory) {

    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    configurer.configure(factory, connectionFactory);
    factory.setMaxMessagesPerTask(-1);
    factory.setSubscriptionDurable(false);
    return factory;
  }

  @Bean
  public DynamicDestinationResolver buildDestinationResolver() {
    return new ByDestinationNameJmsResolver();
  }

  @Bean
  public JmsTemplate buildJmsTemplate(CachingConnectionFactory factory) {

    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(buildDestinationResolver());
    template.setSessionAcknowledgeModeName(acknoledgeMode + "_ACKNOWLEDGE");
    template.setTimeToLive(2000);
    template.setPubSubNoLocal(true);
    template.setDeliveryPersistent(false);
    template.setMessageIdEnabled(false);
    template.setMessageTimestampEnabled(false);

    return template;
  }
}
