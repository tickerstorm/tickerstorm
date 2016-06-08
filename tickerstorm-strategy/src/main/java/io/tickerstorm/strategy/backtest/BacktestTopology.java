package io.tickerstorm.strategy.backtest;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;



@Service
public class BacktestTopology {

  private Config stormConfig = new Config();

  @Autowired
  private ForwardFlowTopologyFactory forwardTopology;

  @Autowired
  private RetroFlowTopologyFactory retroTopology;

  private LocalCluster cluster;
  private final String FORWARD = "forward-flow-topology";
  private final String RETRO = "retro-flow-topology";

  @PostConstruct
  public void init() throws Exception {

    stormConfig.setDebug(false);
    stormConfig.setNumWorkers(1);

    cluster = new LocalCluster();
    cluster.submitTopology(FORWARD, stormConfig, forwardTopology.withBasicStatsBolt().storeToModelData().build());
    cluster.submitTopology(RETRO, stormConfig, retroTopology.build());

  }

  @PreDestroy
  private void destroy() {

    if (cluster != null) {
      cluster.killTopology(FORWARD);
      cluster.killTopology(RETRO);
    }

  }

}
