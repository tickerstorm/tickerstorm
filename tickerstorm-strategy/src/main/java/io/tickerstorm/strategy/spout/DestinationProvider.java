package io.tickerstorm.strategy.spout;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jms.JmsProvider;
import org.springframework.beans.factory.annotation.Autowired;

@SuppressWarnings("serial")
public class DestinationProvider implements JmsProvider {

  public DestinationProvider(ConnectionFactory factory, String destination) throws Exception {
    this.factory = factory;

    if (StringUtils.startsWithIgnoreCase(destination, "topic."))
      this.destination = factory.createConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE).createTopic(destination);

    if (StringUtils.startsWithIgnoreCase(destination, "queue."))
      this.destination = factory.createConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE).createQueue(destination);
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
