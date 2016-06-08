package io.tickerstorm.strategy.spout;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;

@SuppressWarnings("serial")
public class MarketDataTupleProducer implements JmsTupleProducer {

  @Override
  public Values toTuple(Message msg) throws JMSException {

    Values v = null;

    if (ObjectMessage.class.isAssignableFrom(msg.getClass())) {

      Object payload = ((ObjectMessage) msg).getObject();

      if (payload != null && MarketData.class.isAssignableFrom(payload.getClass())) {

        MarketData md = (MarketData) payload;
        v = new Values(md);
      }
    }

    return v;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Field.Name.MARKETDATA.field()));
  }


}
