package io.tickerstorm.common;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.util.ErrorHandler;

import com.google.common.base.Throwables;

import io.tickerstorm.common.eventbus.ByDestinationNameJmsResolver;

@EnableJms
@Configuration
@PropertySource({"classpath:/default.properties"})
public class JmsEventBusContext {

  private static final Logger logger = LoggerFactory.getLogger(JmsEventBusContext.class);

  @Value("${jms.transport}")
  protected String transport;
  
  @Value("${service.name}")
  protected String serviceName;

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    connectionFactory.setOptimizeAcknowledge(true);
    connectionFactory.setAlwaysSessionAsync(false);
    connectionFactory.setClientID(serviceName);
        
    RedeliveryPolicy policy = connectionFactory.getRedeliveryPolicy();
    policy.setInitialRedeliveryDelay(500);
    policy.setBackOffMultiplier(2);
    policy.setUseExponentialBackOff(true);
    policy.setMaximumRedeliveries(2);
    
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
  public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory cf) {
    
    
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    factory.setConnectionFactory(cf);
    factory.setErrorHandler(new ErrorHandler() {
      @Override
      public void handleError(Throwable arg0) {
        logger.error(Throwables.getRootCause(arg0).getMessage(), Throwables.getRootCause(arg0));

      }
    });
    
    factory.setDestinationResolver(new ByDestinationNameJmsResolver());
    factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    factory.setMaxMessagesPerTask(-1);
    factory.setClientId(serviceName);
    return factory;
  }

  @Bean
  public JmsTemplate buildJmsTemplate(ConnectionFactory factory) {
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
