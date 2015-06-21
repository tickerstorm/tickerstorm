package io.tickerstorm.messaging;

import io.tickerstorm.entity.MarketData;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.springframework.stereotype.Component;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@Component
@SuppressWarnings("serial")
public class StormJmsTupleProducer implements JmsTupleProducer {

  @Override
  public Values toTuple(Message msg) throws JMSException {

    if (msg instanceof ObjectMessage) {
      MarketData data = (MarketData) ((ObjectMessage) msg).getObject();
      return new Values(data);
    } else {
      return null;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("marketdata"));
  }

}
