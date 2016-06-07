package io.tickerstorm.strategy.backtest;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.bolt.BasicStatsBolt;
import io.tickerstorm.strategy.bolt.CSVWriterBolt;
import io.tickerstorm.strategy.bolt.ClockBolt;
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
  private BasicStatsBolt simpleStatsBolt;

  @Qualifier("notification")
  @Autowired
  private JmsBolt notificationBolt;

  @Qualifier("modelData")
  @Autowired
  private JmsBolt modelDataBolt;

  private final TopologyBuilder builder = new TopologyBuilder();

  protected ForwardFlowTopologyFactory() {}

  private boolean simpleStats = false;
  private boolean csv = false;
  private boolean modelData = false;
  private boolean notifications = false;
  private boolean logger = false;
  private boolean fieldTypeSplitter = false;
  private boolean clock = false;

  @PostConstruct
  private void init() {
    builder.setSpout("marketdata", jmsSpout);
    builder.setSpout("commands", commandsSpout);
  }

  public ForwardFlowTopologyFactory withFieldTypeSplitting() {

    if (fieldTypeSplitter)
      return this;

    fieldTypeSplitter = true;
    attachCommon(builder.setBolt("fieldTypeSplitting", fieldTypeSplitting).localOrShuffleGrouping("marketdata"));
    return this;
  }

  public ForwardFlowTopologyFactory withNotifications() {

    if (notifications)
      return this;

    notifications = true;
    attachCommon(builder.setBolt("notifications", notificationBolt).localOrShuffleGrouping("marketdata"));
    return this;
  }

  public ForwardFlowTopologyFactory withClock() {

    if (clock)
      return this;

    clock = true;
    attachCommon(builder.setBolt("clock", clockBolt).localOrShuffleGrouping("fieldTypeSplitting"));
    return this;
  }

  private BoltDeclarer attachCommon(BoltDeclarer declarer) {
    declarer.allGrouping("commands");
    return declarer;
  }

  public ForwardFlowTopologyFactory withLogginBolt() {

    if (logger)
      return this;

    logger = true;
    attachCommon(builder.setBolt("logger", loggingBolt).localOrShuffleGrouping("simpleStats"));
    return this;
  }

  public ForwardFlowTopologyFactory withBasicStatsBolt() {

    if (simpleStats)
      return this;

    withFieldTypeSplitting();

    simpleStats = true;
    attachCommon(
        builder.setBolt("simpleStats", simpleStatsBolt).localOrShuffleGrouping("fieldTypeSplitting", Field.Name.CONTINOUS_FIELDS.field())
            .localOrShuffleGrouping("fieldTypeSplitting", Field.Name.DISCRETE_FIELDS.field()));
    return this;
  }

  public ForwardFlowTopologyFactory withCSVBolt() {

    if (csv)
      return this;

    csv = true;
    attachCommon(builder.setBolt("csv", csvBolt).localOrShuffleGrouping("simpleStats"));
    return this;
  }

  public ForwardFlowTopologyFactory storeToModelData() {

    if (modelData)
      return this;

    BoltDeclarer dec = builder.setBolt("modelData", modelDataBolt).localOrShuffleGrouping("marketdata");
    
    if (simpleStats) {
      dec.localOrShuffleGrouping("simpleStats");
    }
    
    modelData = true;

    return this;
  }

  public StormTopology build() {
    return builder.createTopology();
  }

}
