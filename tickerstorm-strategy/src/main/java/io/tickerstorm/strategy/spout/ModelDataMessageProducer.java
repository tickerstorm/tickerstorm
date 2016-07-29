package io.tickerstorm.strategy.spout;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.tuple.Tuple;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.bolt.TupleUtil;

/**
 * Only propagates fields, field collections and market data tuple values
 * 
 * @author kkarski
 *
 */
@SuppressWarnings("serial")
public class ModelDataMessageProducer implements JmsMessageProducer {

  @Override
  public Message toMessage(Session session, Tuple input) throws JMSException {

    HashMap<String, Object> fields = TupleUtil.toMap(input);
    Map<String, Object> loopFields = new HashMap<>(fields);
    ObjectMessage m = session.createObjectMessage();

    for (Entry<String, Object> e : loopFields.entrySet()) {
      if (!MarketData.class.isAssignableFrom(e.getValue().getClass()) && !Field.class.isAssignableFrom(e.getValue().getClass())
          && !Collection.class.isAssignableFrom(e.getValue().getClass())) {
        fields.remove(e.getKey()); // remove from first map
      }
    }

    m.setObject(fields);

    return m;
  }

}
