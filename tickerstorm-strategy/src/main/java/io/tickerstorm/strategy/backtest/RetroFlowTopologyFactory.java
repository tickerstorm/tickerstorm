package io.tickerstorm.strategy.backtest;

import javax.annotation.PostConstruct;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;



@Component
public class RetroFlowTopologyFactory {

  @Qualifier("retroModelData")
  @Autowired
  private JmsSpout retroModelData;

  @Qualifier("commands")
  @Autowired
  private JmsSpout commandsSpout;

  private final TopologyBuilder retroBuilder = new TopologyBuilder();

  protected RetroFlowTopologyFactory() {}

  @PostConstruct
  private void init() {
    retroBuilder.setSpout("retroModelData", retroModelData);
    retroBuilder.setSpout("commands", commandsSpout);
  }

  public StormTopology build() {
    return retroBuilder.createTopology();
  }
}
