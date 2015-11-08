package io.tickerstorm.strategy.backtest;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Service;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;
import io.tickerstorm.strategy.BacktestTopologyContext;
import io.tickerstorm.strategy.bolt.CSVWriterBolt;
import io.tickerstorm.strategy.bolt.ClockBolt;
import io.tickerstorm.strategy.bolt.ComputeAverageBolt;
import io.tickerstorm.strategy.bolt.LogginBolt;

@Service
public class BacktestTopology {

  private Config stormConfig = new Config();

  private LocalCluster cluster;

  @Qualifier("realtime")
  @Autowired
  private JmsSpout jmsSpout;

  @Qualifier("commands")
  @Autowired
  private JmsSpout commandsSpout;

  @Autowired
  private ClockBolt clockBolt;

  @Autowired
  private LogginBolt loggingBolt;

  private final String NAME = "storm-topology";

  @Autowired
  private CSVWriterBolt csvBolt;

  @Autowired
  private ComputeAverageBolt aveBolt;

  @Qualifier("notification")
  @Autowired
  private JmsBolt notificationBolt;
  
  @Qualifier("modelData")
  @Autowired
  private JmsBolt modelDataBolt;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(BacktestTopologyContext.class, args);
  }

  @PostConstruct
  public void init() throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("marketdata", jmsSpout);
    builder.setSpout("commands", commandsSpout);
    builder.setBolt("clock", clockBolt).localOrShuffleGrouping("marketdata").allGrouping("commands");
    builder.setBolt("ave", aveBolt).localOrShuffleGrouping("clock").allGrouping("commands");
    builder.setBolt("logger", loggingBolt).localOrShuffleGrouping("ave").allGrouping("commands");
    builder.setBolt("notification", notificationBolt).localOrShuffleGrouping("ave");
    builder.setBolt("modelData", modelDataBolt).localOrShuffleGrouping("ave");

    stormConfig.setDebug(false);
    stormConfig.setNumWorkers(1);

    cluster = new LocalCluster();
    cluster.submitTopology(NAME, stormConfig, builder.createTopology());

  }

  @PreDestroy
  private void destroy() {

    if (cluster != null) {
      cluster.killTopology(NAME);
    }

  }

}
