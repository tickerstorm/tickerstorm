package io.tickerstorm.strategy.spout;

import java.io.Serializable;
import java.util.ArrayList;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class NotificationMessageProducer implements JmsMessageProducer {

  @Override
  public Message toMessage(Session session, Tuple input) throws JMSException {

    Message m = null;
    ArrayList<Serializable> nots = new ArrayList<>();

    for (String f : input.getFields()) {
      if (StringUtils.startsWithIgnoreCase(f, "notification.")) {

        Serializable value = (Serializable) input.getValueByField(f);
        nots.add(value);
        m = session.createObjectMessage(nots);

      }
    }

    return m;
  }

}
