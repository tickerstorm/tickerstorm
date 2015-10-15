package io.tickerstorm.data.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@Repository
@Listener(references = References.Strong)
public class MarketDataCassandraSink {

  private class BatchSaveTask extends TimerTask {

    @Override
    public void run() {

      if (!batch.get().isEmpty())
        persist();
    }

  }

  private static final org.slf4j.Logger logger =
      LoggerFactory.getLogger(MarketDataCassandraSink.class);

  private final Timer timer = new Timer();
  private final AtomicReference<List<MarketDataDto>> batch =
      new AtomicReference<List<MarketDataDto>>(Collections.synchronizedList(new ArrayList<>()));

  private AtomicLong count = new AtomicLong(0);

  private AtomicLong received = new AtomicLong(0);

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @Autowired
  private CassandraOperations session;

  @Autowired
  private MarketDataDao dao;

  @Qualifier("historical")
  @Autowired
  private MBassador<MarketData> historicalBus;

  @PreDestroy
  public void destroy() {
    historicalBus.unsubscribe(this);
    timer.cancel();
    timer.purge();
  }

  @PostConstruct
  public void init() {
    session.execute("USE " + keyspace);
    historicalBus.subscribe(this);
    timer.scheduleAtFixedRate(new BatchSaveTask(), 0, 5000);
  }

  @Handler
  public void onMarketData(MarketData data) {
    try {

      received.incrementAndGet();

      MarketDataDto dto = MarketDataDto.convert(data);

      if (dto != null)
        batch.get().add(dto);

      if (batch.get().size() > 149)
        persist();

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }


  private void persist() {
    try {

      List<MarketDataDto> data = batch.getAndSet(Collections.synchronizedList(new ArrayList<>()));;

      synchronized (data) {
        dao.save(data);
        logger.debug("Persisting " + data.size() + " records, " + count.addAndGet(data.size())
            + " total saved and " + received.get() + " received");
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }
}
