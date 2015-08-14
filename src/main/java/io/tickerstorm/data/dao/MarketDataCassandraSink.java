package io.tickerstorm.data.dao;

import io.tickerstorm.entity.MarketData;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

@Repository
public class MarketDataCassandraSink {

  private static final org.slf4j.Logger logger = LoggerFactory
      .getLogger(MarketDataCassandraSink.class);

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @Autowired
  private CassandraOperations session;

  @PostConstruct
  public void init() {

    session.execute("USE " + keyspace);
    historicalBus.register(this);

  }

  @PreDestroy
  public void destroy() {
    historicalBus.unregister(this);

  }

  @Autowired
  private MarketDataDao dao;

  @Qualifier("historical")
  @Autowired
  private EventBus historicalBus;

  @Subscribe
  public void onMarketData(MarketData data) {

    try {
      MarketDataDto dto = MarketDataDto.convert(data);
      dao.save(dto);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }

  }
}
