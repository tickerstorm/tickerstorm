package io.tickerstorm.data;

import io.tickerstorm.data.jms.ByDestinationNameJmsResolver;
import io.tickerstorm.data.jms.Destinations;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.listener.DefaultMessageListenerContainer;



public class TestMarketDataServiceConfig extends MarketDataServiceConfig {

  @Qualifier("realtime")
  @Bean
  public DefaultMessageListenerContainer buildQueryListenerContainer(ConnectionFactory factory) {

    DefaultMessageListenerContainer container = null;

    try {
      container = new DefaultMessageListenerContainer();
      container.setConnectionFactory(factory);
      container.setDestinationName(Destinations.TOPIC_REALTIME_MARKETDATA);
      container.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
      container.setDestinationResolver(new ByDestinationNameJmsResolver());
    } catch (Exception e) {
      // Nothing, setting listner in test code
    }
    return container;
  }

}
