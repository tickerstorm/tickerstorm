package io.tickerstorm.data.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cassandra.core.AsynchronousQueryListener;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class BaseCassandraSink<T> {

  private class BatchSaveTask extends TimerTask {

    @Override
    public void run() {

      List<T> b = nextBatch();
      if (!b.isEmpty())
        persist(b);

    }
  }

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(BaseCassandraSink.class);
  private final Timer timer = new Timer();
  protected final AtomicReference<List<T>> batch = new AtomicReference<List<T>>(new ArrayList<T>());
  protected final AtomicLong count = new AtomicLong(0);
  protected final AtomicLong received = new AtomicLong(0);
  protected final ExceptionHandlerListener listener = new ExceptionHandlerListener();

  protected int batchSize() {
    return 149;
  }

  @Value("${cassandra.keyspace}")
  protected String keyspace;

  @Autowired
  protected CassandraOperations session;

  public void destroy() {
    timer.cancel();
    timer.purge();
  }

  public void init() {
    session.execute("USE " + keyspace);
    timer.scheduleAtFixedRate(new BatchSaveTask(), 0, 5000);
  }

  protected void batch(T d) {

    if (d == null)
      return;

    synchronized (received) {
      received.addAndGet(1);
    }

    if (batchSize() > 1) {

      batch.get().add(d);

      if (batch.get().size() > batchSize()) {
        List<T> b = nextBatch();
        if (!b.isEmpty()) {
          persist(b);
        }
      }

    } else if (batchSize() <= 1) {
      persist(Sets.newHashSet(d));
    }

  }

  protected void batch(Collection<T> d) {

    if (d == null || d.isEmpty())
      return;

    synchronized (received) {
      received.addAndGet(d.size());
    }

    if (batchSize() > 1) {

      batch.get().addAll((Collection<T>) d);

      if (batch.get().size() > batchSize()) {
        List<T> b = nextBatch();
        if (!b.isEmpty()) {
          persist(b);
        }
      }

    } else if (batchSize() <= 1) {
      persist(d);
    }

  }

  protected abstract void persist(Collection<T> batch);

  protected synchronized List<T> nextBatch() {
    List<T> data = batch.getAndSet(new ArrayList<T>());
    return data;
  }

  private class ExceptionHandlerListener implements AsynchronousQueryListener {

    @Override
    public void onQueryComplete(ResultSetFuture rsf) {
      try {
        List<ExecutionInfo> infos = rsf.get().getAllExecutionInfo();

      } catch (ExecutionException | InterruptedException e) {
        logger.error(e.getMessage(), e);
      }

    }

  }
}
