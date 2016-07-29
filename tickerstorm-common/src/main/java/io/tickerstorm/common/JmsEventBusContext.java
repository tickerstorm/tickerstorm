package io.tickerstorm.common;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.common.data.eventbus.ByDestinationNameJmsResolver;

@EnableJms
@Configuration
@PropertySource({"classpath:default.properties"})
public class JmsEventBusContext {


  @Value("${jms.transport}")
  protected String transport;

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Bean
  public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory cf) {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    factory.setConnectionFactory(cf);
    factory.setDestinationResolver(new ByDestinationNameJmsResolver());
    factory.setConcurrency("1");
    factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    return factory;
  }

  @Bean
  public JmsTemplate buildJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    template.setPubSubNoLocal(true);
    return template;
  }

}
