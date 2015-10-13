package io.tickerstorm.data.eventbus;

import java.util.HashMap;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.springframework.jms.support.destination.DynamicDestinationResolver;
import org.springframework.util.Assert;

public class ByDestinationNameJmsResolver extends DynamicDestinationResolver {

  private final HashMap<String, Destination> cache = new HashMap<String, Destination>();

  @Override
  public Destination resolveDestinationName(Session session, String destinationName,
      boolean pubSubDomain) throws JMSException {

    Assert.notNull(session, "Session must not be null");
    Assert.notNull(destinationName, "Destination name must not be null");

    if (cache.containsKey(destinationName.toLowerCase()))
      return cache.get(destinationName.toLowerCase());

    if (destinationName.toLowerCase().contains("queue")) {
      pubSubDomain = false;
    } else if (destinationName.toLowerCase().contains("topic")) {
      pubSubDomain = true;
    }

    Destination d = null;

    if (pubSubDomain) {
      d = resolveTopic(session, destinationName);
    } else {
      d = resolveQueue(session, destinationName);
    }

    cache.put(destinationName.toLowerCase(), d);
    return d;

  }
}
