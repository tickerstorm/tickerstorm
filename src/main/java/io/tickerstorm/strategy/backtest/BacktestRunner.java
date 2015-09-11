package io.tickerstorm.strategy.backtest;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.strategy.bolt.CSVOutputBolt;
import io.tickerstorm.strategy.bolt.ClockBolt;
import io.tickerstorm.strategy.bolt.ComputeAverageBolt;
import io.tickerstorm.strategy.bolt.LogginBolt;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;


public abstract class BacktestRunner {

  private Config stormConfig = new Config();

  private LocalCluster cluster;

  @Autowired
  private JmsSpout jmsSpout;

  @Autowired
  private ClockBolt clockBolt;

  @Autowired
  private LogginBolt loggingBolt;

  private final String NAME = "storm-topology";

  @Autowired
  private CSVOutputBolt csvBolt;

  @Autowired
  private JmsTemplate queryTemplate;

  @PostConstruct
  public void init() throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("marketdata", jmsSpout);
    builder.setBolt("clock", clockBolt).shuffleGrouping("marketdata");
    builder.setBolt("ave", new ComputeAverageBolt()).shuffleGrouping("clock");
    builder.setBolt("logger", loggingBolt).shuffleGrouping("ave");
    builder.setBolt("csv", csvBolt).shuffleGrouping("ave");

    stormConfig.setDebug(false);
    stormConfig.setNumWorkers(1);

    cluster = new LocalCluster();
    cluster.submitTopology(NAME, stormConfig, builder.createTopology());

    Thread.sleep(1000);

    initData();
  }

  protected abstract void initData();

  protected void sendQuery(HistoricalFeedQuery query) {
    queryTemplate.send(new MessageCreator() {

      @Override
      public Message createMessage(Session session) throws JMSException {
        Message m = session.createObjectMessage(query);
        return m;
      }
    });
  }


  @PreDestroy
  private void destroy() {

    if (cluster != null) {
      cluster.killTopology(NAME);
    }

  }

}
