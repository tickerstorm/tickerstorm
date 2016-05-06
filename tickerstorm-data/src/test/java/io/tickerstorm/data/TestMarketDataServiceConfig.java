package io.tickerstorm.data;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.util.ErrorHandler;

import io.tickerstorm.common.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.common.data.eventbus.Destinations;

@EnableAutoConfiguration
@Configuration
public class TestMarketDataServiceConfig extends MarketDataApplicationContext {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TestMarketDataServiceConfig.class, args);
  }

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
      container.setErrorHandler(new ErrorHandler() {

        @Override
        public void handleError(Throwable t) {
          // nothing
        }
      });
    } catch (Exception e) {
      // Nothing, setting listner in test code
    }
    return container;
  }
}
