package io.tickerstorm.strategy.spout;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.Quote;
import io.tickerstorm.entity.Tick;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class StormJmsTupleProducer implements JmsTupleProducer {

  @Override
  public Values toTuple(Message msg) throws JMSException {

    if (msg instanceof ObjectMessage) {

      MarketData data = (MarketData) ((ObjectMessage) msg).getObject();
      MarketData[] arr = new MarketData[4];

      arr[0] = data;

      if (Candle.TYPE.equals(data.getType()))
        arr[1] = data;

      if (Quote.TYPE.equals(data.getType()))
        arr[2] = data;

      if (Tick.TYPE.equals(data.getType()))
        arr[3] = data;

      return new Values((Object[]) arr);

    } else {
      return null;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Lists.newArrayList(io.tickerstorm.strategy.bolt.Fields
        .marketdataFields())));
  }
}
