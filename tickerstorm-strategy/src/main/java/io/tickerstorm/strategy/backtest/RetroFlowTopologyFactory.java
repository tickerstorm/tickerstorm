package io.tickerstorm.strategy.backtest;

import javax.annotation.PostConstruct;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;



public class RetroFlowTopologyFactory {

  @Qualifier("retroModelData")
  @Autowired
  private JmsSpout retroModelData;

  @Qualifier("commands")
  @Autowired
  private JmsSpout commandsSpout;

  @Qualifier("modelData")
  @Autowired
  private JmsBolt modelDataBolt;

  private final TopologyBuilder retroBuilder = new TopologyBuilder();

  protected RetroFlowTopologyFactory() {}

  private boolean modelData;

  @PostConstruct
  private void init() {
    retroBuilder.setSpout("retroModelData", retroModelData);
    retroBuilder.setSpout("commands", commandsSpout);
  }

  public RetroFlowTopologyFactory storeToModelData() {

    if (modelData)
      return this;

    //Should not stream back model events since they are already stored. Only store new generated events
    //BoltDeclarer dec = retroBuilder.setBolt("modelData", modelDataBolt).localOrShuffleGrouping("retroModelData");

    modelData = true;

    return this;
  }

  public StormTopology build() {
    return retroBuilder.createTopology();
  }
}
