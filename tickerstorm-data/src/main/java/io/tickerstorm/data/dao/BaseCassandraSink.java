/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.data.dao;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Sets;
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

      batch.get().addAll(d);

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
