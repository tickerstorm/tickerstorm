package io.tickerstorm.strategy.spout;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.springframework.beans.factory.annotation.Autowired;

import backtype.storm.contrib.jms.JmsProvider;

@SuppressWarnings("serial")
public class RealtimeDestinationProvider implements JmsProvider {

  public RealtimeDestinationProvider(ConnectionFactory factory, Destination realtime) {
    this.factory = factory;
    this.destination = realtime;
  }

  @Autowired
  private ConnectionFactory factory;


  @Autowired
  private Destination destination;

  @Override
  public ConnectionFactory connectionFactory() throws Exception {
    return factory;
  }

  @Override
  public Destination destination() throws Exception {
    return destination;
  }

}
