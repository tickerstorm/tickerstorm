package io.tickerstorm.strategy.util;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BacktestClock {

  private AtomicReference<Instant> ref = new AtomicReference<Instant>();

  private final static Logger logger = LoggerFactory.getLogger(BacktestClock.class);

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
      logger.trace("Time is now: " + ref.get().toString());
    }

    return ref.get();
  }


}
