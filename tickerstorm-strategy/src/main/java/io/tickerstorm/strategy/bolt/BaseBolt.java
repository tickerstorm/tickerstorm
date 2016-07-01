package io.tickerstorm.strategy.bolt;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.MarketData;

@SuppressWarnings("serial")
public abstract class BaseBolt extends BaseRichBolt {

  protected OutputCollector coll = null;
  protected Map config;
  protected TopologyContext context;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.coll = collector;
    this.config = stormConf;
    this.context = context;
    init();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}

  protected void ack(Tuple t) {
    if (coll != null)
      coll.ack(t);
  }

  protected void executeCommand(Tuple t, Command input) {
    ack(t);
  }

  protected void executeMarker(Tuple t, Marker input) {
    ack(t);
  }

  protected void executeMarketData(Tuple t) {
    ack(t);
  }

  @Override
  public final void execute(Tuple input) {

    if (input.contains(Field.Name.MARKER.field())) {

      Object o = input.getValueByField(Field.Name.MARKER.field());

      if (Command.class.isAssignableFrom(o.getClass())) {
        Command m = (Command) o;
        executeCommand(input, m);
      } else {
        Marker m = (Marker) o;
        executeMarker(input, m);
      }
    }

    if (input.contains(Field.Name.MARKETDATA.field())) {
      executeMarketData(input);
    }

    process(input);
  }

  protected void process(Tuple input) {
    ack(input);
  }
  protected void init() {}

}
