package io.tickerstorm.common;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.util.ErrorHandler;

import com.google.common.base.Throwables;

import io.tickerstorm.common.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.common.eventbus.Destinations;

@EnableJms
@Configuration
@PropertySource({"classpath:/default.properties"})
public class JmsEventBusContext {

  private static final Logger logger = LoggerFactory.getLogger(JmsEventBusContext.class);

  @Value("${jms.transport}")
  protected String transport;

  @Value("${jms.concurrency}")
  protected String concurrency;

  @Value("${jms.messagesPerTask}")
  protected Integer messagesPerTask;

  @Value("${service.name}")
  protected String serviceName;

  @Bean
  public CachingConnectionFactory buildConnectionFactoryCache() {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    connectionFactory.setAlwaysSessionAsync(false);
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
  public DefaultMessageListenerContainer getJmsListenerContainer() {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    connectionFactory.setAlwaysSessionAsync(false);
    DefaultMessageListenerContainer factory = new DefaultMessageListenerContainer();
    factory.setConnectionFactory(connectionFactory);
    factory.setErrorHandler(new ErrorHandler() {
      @Override
      public void handleError(Throwable arg0) {
        logger.error(Throwables.getRootCause(arg0).getMessage(), Throwables.getRootCause(arg0));

      }
    });

    factory.setClientId(serviceName);
    factory.setDestinationName(Destinations.TOPIC_REALTIME_MARKETDATA);
    factory.setDestinationResolver(new ByDestinationNameJmsResolver());
    factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    factory.setConcurrency(concurrency);
    factory.setAutoStartup(false);
    factory.setMaxMessagesPerTask(messagesPerTask);

    return factory;
  }

  @Bean
  public JmsTemplate buildJmsTemplate(CachingConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    template.setPubSubNoLocal(true);
    template.setDeliveryPersistent(false);
    template.setMessageIdEnabled(false);
    template.setMessageTimestampEnabled(false);

    return template;
  }

}
