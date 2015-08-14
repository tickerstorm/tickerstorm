package io.tickerstorm.strategy.spout;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

import backtype.storm.contrib.jms.JmsProvider;

@Qualifier("realtime")
@Component
@SuppressWarnings("serial")
public class RealtimeDestinationProvider implements JmsProvider {

  @Qualifier("realtime")
  @Autowired
  private DefaultMessageListenerContainer container;

  @Override
  public ConnectionFactory connectionFactory() throws Exception {
    return container.getConnectionFactory();
  }

  @Override
  public Destination destination() throws Exception {
    return container.getDestination();
  }

}
