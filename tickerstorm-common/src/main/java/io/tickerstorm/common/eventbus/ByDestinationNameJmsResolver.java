package io.tickerstorm.common.eventbus;

import java.util.HashMap;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import org.springframework.util.Assert;

public class ByDestinationNameJmsResolver {

  public static Destination resolveDestinationName(Session session, String destinationName) throws JMSException {

    Assert.notNull(session, "Session must not be null");
    Assert.notNull(destinationName, "Destination name must not be null");

    boolean pubSubDomain = false;

    if (destinationName.toLowerCase().startsWith("consumer.")) {
      pubSubDomain = false;
    } else if (destinationName.toLowerCase().contains("queue")) {
      pubSubDomain = false;
    } else if (destinationName.toLowerCase().contains("topic")) {
      pubSubDomain = true;
    }

    Destination d = null;

    if (pubSubDomain) {
      d = session.createTopic(destinationName);
    } else {
      d = session.createQueue(destinationName);
    }

    return d;

  }
}
