package io.tickerstorm.strategy.bolt;

import io.tickerstorm.entity.MarketData;

import java.time.Instant;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@Component
@SuppressWarnings("serial")
public class LogginBolt extends BaseRichBolt {

  private final static Logger logger = LoggerFactory.getLogger(LogginBolt.class);
  private OutputCollector coll;

  @Override
  public void execute(Tuple tuple) {

    MarketData e = (MarketData) tuple.getValueByField(Fields.MARKETDATA.fieldName());
    Instant clock = (Instant) tuple.getValueByField(Fields.NOW.fieldName());

    logger.info("Market event at" + clock.toString() + " Event:  " + e.toString());
    coll.ack(tuple);
  }

  @Override
  public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
    this.coll = arg2;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer arg0) {}

}
