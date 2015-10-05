package io.tickerstorm.data;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import io.tickerstorm.data.jms.ByDestinationNameJmsResolver;
import io.tickerstorm.data.jms.Destinations;



public class TestMarketDataServiceConfig extends MarketDataServiceConfig {

  @Qualifier("realtime")
  @Bean
  @DependsOn(value = {"brokerService"})
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

  @Bean(initMethod = "start", destroyMethod = "stop", name = "brokerService")
  public BrokerService activemqBroker() throws Exception {
    BrokerService service = new BrokerService();
    service.addConnector(transport);
    return service;
  }
}
