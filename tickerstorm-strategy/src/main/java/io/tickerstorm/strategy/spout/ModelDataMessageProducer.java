package io.tickerstorm.strategy.spout;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.util.TupleUtil;

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

    Map<String, Object> fields = TupleUtil.toMap(input);
    MapMessage m = session.createMapMessage();

    for (Entry<String, Object> e : fields.entrySet()) {

      if (MarketData.class.isAssignableFrom(e.getValue().getClass()) && Field.class.isAssignableFrom(e.getValue().getClass())
          && Collection.class.isAssignableFrom(e.getValue().getClass())) {
        m.setObjectProperty(e.getKey(), e.getValue());
      }
    }

    return m;
  }

}
