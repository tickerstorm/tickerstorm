package io.tickerstorm.data.dao;

import io.tickerstorm.entity.MarketData;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

@Repository
@Listener(references = References.Strong)
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
    historicalBus.subscribe(this);

  }

  @PreDestroy
  public void destroy() {
    historicalBus.unsubscribe(this);

  }

  @Autowired
  private MarketDataDao dao;

  @Qualifier("historical")
  @Autowired
  private MBassador<MarketData> historicalBus;

  @Handler
  public void onMarketData(MarketData data) {

    try {
      MarketDataDto dto = MarketDataDto.convert(data);
      dao.save(dto);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }

  }
}
