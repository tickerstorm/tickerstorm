package io.tickerstorm.strategy.backtest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

@Component
public class RetroFlowTopologyFactory {

  @Qualifier("retroModelData")
  @Autowired
  private JmsSpout retroModelData;

  @Qualifier("commands")
  @Autowired
  private JmsSpout commandsSpout;

  private final TopologyBuilder retroBuilder = new TopologyBuilder();

  protected RetroFlowTopologyFactory() {

    retroBuilder.setSpout("retroModelData", retroModelData);
    retroBuilder.setSpout("commands", commandsSpout);
  }

  public StormTopology build() {
    return retroBuilder.createTopology();
  }
}
