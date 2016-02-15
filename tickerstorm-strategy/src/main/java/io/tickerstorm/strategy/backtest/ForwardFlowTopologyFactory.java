package io.tickerstorm.strategy.backtest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.bolt.CSVWriterBolt;
import io.tickerstorm.strategy.bolt.ClockBolt;
import io.tickerstorm.strategy.bolt.ComputeSimpleStatsBolt;
import io.tickerstorm.strategy.bolt.FieldTypeSplittingBolt;
import io.tickerstorm.strategy.bolt.LogginBolt;

@Component
public class ForwardFlowTopologyFactory {

  @Autowired
  private ClockBolt clockBolt;

  @Autowired
  private LogginBolt loggingBolt;

  @Autowired
  private FieldTypeSplittingBolt fieldTypeSplitting;

  @Qualifier("realtime")
  @Autowired
  private JmsSpout jmsSpout;

  @Qualifier("commands")
  @Autowired
  private JmsSpout commandsSpout;

  @Autowired
  private CSVWriterBolt csvBolt;

  @Autowired
  private ComputeSimpleStatsBolt aveBolt;

  @Qualifier("notification")
  @Autowired
  private JmsBolt notificationBolt;

  @Qualifier("modelData")
  @Autowired
  private JmsBolt modelDataBolt;

  private final TopologyBuilder builder = new TopologyBuilder();

  protected ForwardFlowTopologyFactory() {

    builder.setSpout("marketdata", jmsSpout);
    builder.setSpout("commands", commandsSpout);
    builder.setBolt("notification", notificationBolt).localOrShuffleGrouping("ave").allGrouping("commands");
    builder.setBolt("clock", clockBolt).localOrShuffleGrouping("fieldTypeSplitting").allGrouping("commands");
    builder.setBolt("fieldTypeSplitting", fieldTypeSplitting).localOrShuffleGrouping("marketdata").allGrouping("commands");
  }

  public ForwardFlowTopologyFactory withLogginBolt() {
    builder.setBolt("logger", loggingBolt).localOrShuffleGrouping("ave").allGrouping("commands");
    return this;
  }

  public ForwardFlowTopologyFactory withAveBolt() {
    builder.setBolt("ave", aveBolt).localOrShuffleGrouping("fieldTypeSplitting", Field.Name.CONTINOUS_FIELDS.field())
        .localOrShuffleGrouping("fieldTypeSplitting", Field.Name.DISCRETE_FIELDS.field()).allGrouping("commands");
    return this;
  }

  public ForwardFlowTopologyFactory withCSVBolt() {
    builder.setBolt("ave", csvBolt).localOrShuffleGrouping("ave").allGrouping("commands");
    return this;
  }

  public ForwardFlowTopologyFactory withModelDataBolt() {
    builder.setBolt("modelData", modelDataBolt).localOrShuffleGrouping("ave").allGrouping("commands");
    return this;
  }

  public StormTopology build() {
    return builder.createTopology();
  }

}
