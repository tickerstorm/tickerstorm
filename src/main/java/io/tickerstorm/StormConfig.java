package io.tickerstorm;

import io.tickerstorm.messaging.StormHistoricalJmsDestinationProvider;
import io.tickerstorm.messaging.StormJmsTupleProducer;

import javax.jms.Session;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import backtype.storm.contrib.jms.spout.JmsSpout;

@ComponentScan(basePackages = { "io.tickerstorm" })
@Configuration
public class StormConfig {

  @Qualifier("historical")
  @Bean
  public JmsSpout buildJmsSpout(@Qualifier("historical") StormHistoricalJmsDestinationProvider provider, StormJmsTupleProducer producer) {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(provider);
    spout.setJmsTupleProducer(producer);
    spout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    spout.setDistributed(false);
    spout.setRecoveryPeriod(1000);
    return spout;

  }

}
