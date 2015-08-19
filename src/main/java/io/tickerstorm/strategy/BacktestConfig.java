package io.tickerstorm.strategy;

import io.tickerstorm.data.messaging.Destinations;
import io.tickerstorm.strategy.spout.RealtimeDestinationProvider;
import io.tickerstorm.strategy.spout.StormJmsTupleProducer;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;

import backtype.storm.contrib.jms.spout.JmsSpout;

@EnableJms
@Configuration
@ComponentScan(basePackages = {"io.tickerstorm.strategy"})
@PropertySource({"classpath:default.properties"})
public class BacktestConfig {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(BacktestConfig.class);

  @Value("${jms.transport}")
  private String transport;

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    logger.info("Creating Connection Factory");
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Qualifier("realtime")
  @Bean
  public Destination buildRealtimeDestination(ConnectionFactory factory) throws Exception {
    return factory.createConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE)
        .createTopic(Destinations.TOPIC_REALTIME_MARKETDATA);
  }

  @Qualifier("realtime")
  @Bean
  public JmsSpout buildJmsSpout(@Qualifier("realtime") RealtimeDestinationProvider provider,
      StormJmsTupleProducer producer) {

    JmsSpout spout = new JmsSpout();
    spout.setJmsProvider(provider);
    spout.setJmsTupleProducer(producer);
    spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout.setDistributed(false);
    spout.setRecoveryPeriod(1000);
    return spout;

  }

  @Bean
  public Clock backtestClock() {
    return new BacktestClock();
  }

  @Bean
  public CacheManager buildCache() {

    CacheConfiguration config =
        new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
            .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO)
            .persistence(new PersistenceConfiguration().strategy(Strategy.NONE));
    config.setName("timeseries");
    CacheManager manager = CacheManager.create();
    manager.addCache(new Cache(config));

    return manager;
  }
}
