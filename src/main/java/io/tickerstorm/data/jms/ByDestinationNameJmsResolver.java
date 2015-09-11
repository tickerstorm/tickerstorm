package io.tickerstorm.data.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.springframework.jms.support.destination.DynamicDestinationResolver;
import org.springframework.util.Assert;

public class ByDestinationNameJmsResolver extends DynamicDestinationResolver {

  @Override
  public Destination resolveDestinationName(Session session, String destinationName,
      boolean pubSubDomain) throws JMSException {

    Assert.notNull(session, "Session must not be null");
    Assert.notNull(destinationName, "Destination name must not be null");

    if (destinationName.toLowerCase().contains("queue")) {
      pubSubDomain = false;
    } else if (destinationName.toLowerCase().contains("topic")) {
      pubSubDomain = true;
    }

    if (pubSubDomain) {
      return resolveTopic(session, destinationName);
    } else {
      return resolveQueue(session, destinationName);
    }


  }
}
