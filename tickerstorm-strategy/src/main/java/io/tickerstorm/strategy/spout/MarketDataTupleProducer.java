package io.tickerstorm.strategy.spout;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;

@SuppressWarnings("serial")
public class MarketDataTupleProducer implements JmsTupleProducer {

  @Override
  public Values toTuple(Message msg) throws JMSException {

    Values v = new Values();

    if (ObjectMessage.class.isAssignableFrom(msg.getClass())) {

      Object payload = ((ObjectMessage) msg).getObject();

      if (payload != null && MarketData.class.isAssignableFrom(payload.getClass())) {

        MarketData md = (MarketData) payload;
        v.add(0, md);
      }
      return v;

    } else {
      return null;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(
        new Fields(Field.Name.MARKETDATA.field()));
  }
}
