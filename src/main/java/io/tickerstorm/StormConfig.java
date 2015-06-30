package io.tickerstorm;

import io.tickerstorm.storm.StormJmsDestinationProvider;
import io.tickerstorm.storm.StormJmsTupleProducer;

import javax.jms.Session;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import backtype.storm.contrib.jms.spout.JmsSpout;

@ComponentScan(basePackages = { "io.tickerstorm.storm" })
@Import({ CommonConfig.class })
@PropertySource({ "classpath:default.properties" })
@Configuration
public class StormConfig {

  @Qualifier("realtime")
  @Bean
  public JmsSpout buildJmsSpout(@Qualifier("realtime") StormJmsDestinationProvider provider, StormJmsTupleProducer producer) {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(provider);
    spout.setJmsTupleProducer(producer);
    spout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    spout.setDistributed(false);
    spout.setRecoveryPeriod(1000);
    return spout;

  }

  @Bean
  public CacheManager buildCache() {

    CacheConfiguration config = new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
        .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO).persistence(new PersistenceConfiguration().strategy(Strategy.NONE));
    config.setName("timeseries");
    CacheManager manager = CacheManager.create();
    manager.addCache(new Cache(config));

    return manager;
  }
}
