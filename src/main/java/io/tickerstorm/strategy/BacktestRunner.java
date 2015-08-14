package io.tickerstorm.strategy;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.strategy.bolt.CSVOutputBolt;
import io.tickerstorm.strategy.bolt.ClockBolt;
import io.tickerstorm.strategy.bolt.ComputeAverageBolt;
import io.tickerstorm.strategy.bolt.LogginBolt;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;

@Component
@SpringBootApplication
public class BacktestRunner {

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

  @Qualifier("query")
  @Autowired
  private JmsTemplate queryTemplate;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(BacktestConfig.class, args);
  }

  @PostConstruct
  public void init() {

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("marketdata", jmsSpout, 1);
    builder.setBolt("clock", clockBolt, 1).shuffleGrouping("marketdata");
    builder.setBolt("ave", new ComputeAverageBolt(), 1).shuffleGrouping("clock");
    builder.setBolt("logger", loggingBolt, 1).shuffleGrouping("ave");
    builder.setBolt("csv", csvBolt, 1).shuffleGrouping("ave");

    stormConfig.setDebug(true);
    stormConfig.setNumWorkers(1);

    cluster = new LocalCluster();
    cluster.submitTopology(NAME, stormConfig, builder.createTopology());

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 20, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);

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
