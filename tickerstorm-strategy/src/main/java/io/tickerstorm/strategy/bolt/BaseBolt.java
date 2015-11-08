package io.tickerstorm.strategy.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public abstract class BaseBolt extends BaseRichBolt {
  
  protected OutputCollector coll = null;
  protected Map config;
  protected TopologyContext context;
  protected Tuple t;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.coll = collector;
    this.config = stormConf;
    this.context = context;
    init();
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //none
  }

  protected void ack(Tuple tuple) {
    coll.ack(tuple);
  }
  
  protected void emit(Object... tuple){
    coll.emit(t, new Values(tuple));
  }

  @Override
  public final void execute(Tuple input) {
    this.t = input;
    process(input);
  }

  protected abstract void process(Tuple input);
  
  protected void init(){}


}
