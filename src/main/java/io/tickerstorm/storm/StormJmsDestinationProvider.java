package io.tickerstorm.storm;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import backtype.storm.contrib.jms.JmsProvider;

@Qualifier("realtime")
@Component
@SuppressWarnings("serial")
public class StormJmsDestinationProvider implements JmsProvider {

  @Qualifier("realtime")
  @Autowired
  private Destination destination;

  @Autowired
  private ConnectionFactory connectionFactory;

  @Override
  public ConnectionFactory connectionFactory() throws Exception {
    return connectionFactory;
  }

  @Override
  public Destination destination() throws Exception {
    return destination;
  }

}
