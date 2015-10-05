package io.tickerstorm.data;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.data.jms.ByDestinationNameJmsResolver;

@EnableJms
@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.data"})
@Import({CommonConfig.class})
public class MarketDataServiceConfig {

  public static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(MarketDataServiceConfig.class);

  @Value("${jms.transport}")
  protected String transport;

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.info("Creating Connection Factory");
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Bean
  public JmsTemplate buildJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    return template;
  }

}
