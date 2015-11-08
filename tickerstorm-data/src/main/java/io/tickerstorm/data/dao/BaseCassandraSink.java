package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;

import net.engio.mbassy.listener.Handler;

public abstract class BaseCassandraSink {

  private class BatchSaveTask extends TimerTask {

    @Override
    public void run() {

      if (!batch.get().isEmpty())
        persist(nextBatch());
    }
  }

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(BaseCassandraSink.class);
  private final Timer timer = new Timer();
  protected final AtomicReference<List<Object>> batch = new AtomicReference<List<Object>>(Collections.synchronizedList(new ArrayList<>()));
  protected final AtomicLong count = new AtomicLong(0);
  protected final AtomicLong received = new AtomicLong(0);

  protected int batchSize = 149;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @Autowired
  private CassandraOperations session;

  public void destroy() {
    timer.cancel();
    timer.purge();
  }

  public void init() {
    session.execute("USE " + keyspace);
    timer.scheduleAtFixedRate(new BatchSaveTask(), 0, 5000);
  }

  @Handler
  public final void onData(Serializable data) {

    try {

      received.incrementAndGet();

      Object d = convert(data);

      if (d != null)
        batch.get().add(d);

      if (batch.get().size() > batchSize)
        persist(nextBatch());

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }

  }

  protected abstract void persist(List<Object> batch);

  protected Serializable convert(Serializable data) {
    return data;
  }

  protected List<Object> nextBatch() {
    List<Object> data = batch.getAndSet(Collections.synchronizedList(new ArrayList<>()));
    return data;
  }
}
