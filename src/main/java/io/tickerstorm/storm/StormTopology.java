package io.tickerstorm.storm;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;

@Component
public class StormTopology {

  private Config stormConfig = new Config();

  private LocalCluster cluster;

  @Autowired
  private JmsSpout jmsSpout;

  @PostConstruct
  private void init() throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("jms", jmsSpout, 1);
    builder.setBolt("logger", new LogginBolt(), 1).shuffleGrouping("jms");

    stormConfig.setDebug(true);     
    stormConfig.setNumWorkers(2);

    cluster = new LocalCluster();
    cluster.submitTopology("storm-topology", stormConfig, builder.createTopology());

  }

  @PreDestroy
  private void destroy() {

    if (cluster != null) {
      cluster.killTopology("storm-topology");
    }

  }

}
