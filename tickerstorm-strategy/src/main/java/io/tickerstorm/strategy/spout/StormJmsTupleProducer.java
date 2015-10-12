package io.tickerstorm.strategy.spout;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import io.tickerstorm.entity.Marker;
import io.tickerstorm.entity.MarketData;

@SuppressWarnings("serial")
public class StormJmsTupleProducer implements JmsTupleProducer {

  @Override
  public Values toTuple(Message msg) throws JMSException {

    Values v = new Values();

    if (ObjectMessage.class.isAssignableFrom(msg.getClass())) {

      Object payload = ((ObjectMessage) msg).getObject();

      if (MarketData.class.isAssignableFrom(payload.getClass())) {
        v.add(0, payload);
      } else if (Marker.class.isAssignableFrom(payload.getClass())) {
        v.add(1, payload);
      }

      return v;

    } else {
      return null;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(io.tickerstorm.strategy.bolt.Fields.MARKETDATA.fieldName(),
        io.tickerstorm.strategy.bolt.Fields.MARKER.fieldName()));
  }
}
