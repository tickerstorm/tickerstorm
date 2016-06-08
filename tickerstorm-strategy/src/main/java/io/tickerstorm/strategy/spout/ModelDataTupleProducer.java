package io.tickerstorm.strategy.spout;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Marker;

@SuppressWarnings("serial")
public class ModelDataTupleProducer implements JmsTupleProducer {

  @Override
  public Values toTuple(Message msg) throws JMSException {

    Map<String, Object> collections = new HashMap<>();

    Values v = new Values();

    if (ObjectMessage.class.isAssignableFrom(msg.getClass())) {

      Object payload = ((ObjectMessage) msg).getObject();

      if (payload != null && Map.class.isAssignableFrom(payload.getClass()) && !Marker.class.isAssignableFrom(payload.getClass()))
        v.add(0, payload);

      return v;

    } else if (MapMessage.class.isAssignableFrom(msg.getClass())) {


    }
    return null;

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Field.Name.FEATURES.field()));
  }
}
