package io.tickerstorm.strategy.util;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class BacktestClock implements Clock, Serializable {

  private AtomicReference<Instant> ref = new AtomicReference<Instant>();

  private final static Logger logger = LoggerFactory.getLogger(BacktestClock.class);

  @Override
  public Instant now() {
    return ref.get();
  }

  public Instant update(Instant dt) {

    if (ref.get() == null) {
      ref.set(dt);
    }

    Instant c = ref.get();

    if (c != null && dt != null && dt.isAfter(c)) {
      ref.compareAndSet(c, dt);
      logger.debug("Time is now: " + ref.get().toString());
    }

    return ref.get();
  }


}
