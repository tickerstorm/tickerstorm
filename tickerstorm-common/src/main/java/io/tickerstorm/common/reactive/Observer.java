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

package io.tickerstorm.common.reactive;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.Markers;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for the purpose of monitoring a notification stream in or to determine when a
 * process has completed based on the published notifications.
 *
 * For example, when a historical feed query is dispatched, the historical feed service will fetch
 * the data and publish it over a stream. In this case, there will be a start notification and end
 * notification with possible update notifications in between. This class would monitor for the end
 * event and report completion. Additionally, if the notification source publishes updates as to
 * completion counts, this class aggregates those statistics in order to report a completion
 * percentage.
 *
 * The completion tracker triggers callback methods upon the completion or timeout of the operation.
 * The onCompletion callback is issued if the notification stream reaches completion within the
 * allotted time frame. The onTimeout callback is triggered if the tracker times out before
 * completion is reached.
 *
 * The completion tracker provides some guarantees as to running time by providing two timeout
 * mechanism. First, the monitored event stream must start. If the startConditino isn't triggered
 * within the timeout time frame, the tracker will report failure. Second, it's not always possible
 * to know when a stream has completed and the only thing to do is to wait until some interval has
 * passed during which no more correlated events have been seen. I.e. a market data stream has been
 * fully processed by the strategy service and the last event has been issued by the startegy
 * service. This can only be tracked by simply waiting out the stream. The second timer mechanism
 * delays the tracker timeout by extending the timeout period by the delay amount after each
 * correlated event.
 *
 * @author kkarski
 */
public class Observer implements CompletionTracker {

  private final static Logger logger = LoggerFactory.getLogger(Observer.class);
  private static final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
  private final AtomicInteger completed = new AtomicInteger(0);
  private final AtomicInteger expect = new AtomicInteger(0);
  private final AtomicInteger failed = new AtomicInteger(0);
  private final EventBus notificationsBus;
  private final AtomicBoolean timedout = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final Timeout task = new Timeout();
  private Predicate<Notification> resetCondition;
  private Predicate<Notification> timerStart;
  private Predicate<Notification> completionCondition;
  private Predicate<Notification> failCondition;
  private long delay = 500;
  private Handler onCompletion;
  private Runnable onTimeout;
  private Runnable onFailure;
  private ScheduledFuture<?> future;
  private ScheduledFuture<?> mustStart;
  private Notification not;
  private String name = "";

  /**
   * @param startCondition Predicate to determine when the first correlated event has occurred to start the completion timer. I.e. the query start event was
   * observed.
   * @param resetCondition Predicate to determine when to extend the timeout period by the delay amount. I.e. to wait out when all market data has streamed
   * through the strategy
   * @param notificationsBus The notification bus over which all events will be received
   * @param delay The delay by which to extend the timeout each time we see a correlated event
   * @param timeout The amount of time to wait before the start condition must have been met. Prevents the tracker from waiting forever on an event which will
   * never come
   * @param onCompletion The callback on completion
   * @param onTimeout The callback on timeout
   */
  protected Observer(EventBus notificationBus) {
    this.notificationsBus = notificationBus;
  }

  public static Observer observe(EventBus notificationBus) {
    Observer handler = new Observer(notificationBus);
    return handler;
  }

  public static Observer observe(EventBus notificationBus, String name) {
    Observer handler = new Observer(notificationBus);
    handler.name = name;
    return handler;
  }

  public Observer startCountDownOn(Predicate<Notification> startCondition) {
    this.timerStart = startCondition;
    return this;
  }

  public Observer extendTimeoutOn(Predicate<Notification> resetCondition, long delay) {
    this.resetCondition = resetCondition;
    this.delay = delay;
    return this;
  }

  public Observer completeWhen(Predicate<Notification> completionCondition) {
    this.completionCondition = completionCondition;
    return this;
  }

  public Observer failedWhen(Predicate<Notification> failureCondition) {
    this.failCondition = failureCondition;
    return this;
  }

  public Observer mustCompleteWithin(long delay) {
    this.delay = delay;
    return this;
  }

  public Observer whenTimedOut(Runnable onTimeout) {
    this.onTimeout = onTimeout;
    return this;
  }

  public Observer whenFailed(Runnable onTimeout) {
    this.onFailure = onTimeout;
    return this;
  }

  public Observer whenComplete(Handler onCompletion) {
    this.onCompletion = onCompletion;
    return this;
  }

  public void start() {
    this.notificationsBus.register(this);
    this.mustStart = timer.schedule(task, delay, TimeUnit.MILLISECONDS);
  }

  @Subscribe
  public void onNotification(Notification not) {

    if (timerStart != null && timerStart.test(not) && future == null && !this.started.get()) {
      logger.info("Tracker " + name + " started");
      this.started.set(true);
      reset();
    }

    if (resetCondition != null && resetCondition.test(not) && this.started.get() && !isDone() && !timedOut()) {

      if (not.is(Markers.START.toString()) && not.expect > 0) {

        expect.set(not.expect);

      } else if (not.is(Markers.SUCCESS.toString()) && not.expect > 0) {

        completed.getAndAdd(not.expect);

      } else if (not.is(Markers.FAILED.toString()) && not.expect > 0) {

        failed.getAndAdd(not.expect);

      }

      reset();

    }

    if (completionCondition != null && completionCondition.test(not) && this.started.get() && !done.get() && !timedOut()) {

      logger.info("Tracker " + name + " completed");

      try {
        notificationsBus.unregister(this);
      } catch (IllegalArgumentException e) {
        //ignore if not registered
      }
      done.compareAndSet(false, true);
      this.not = not;

      if (future != null && !future.isCancelled()) {
        future.cancel(false);
      }

      if (mustStart != null && !mustStart.isCancelled()) {
        mustStart.cancel(false);
      }

      if (onCompletion != null) {
        onCompletion.handle(not);
      }
    }

    if (failCondition != null && failCondition.test(not) && !isDone() && !timedOut()) {

      logger.info("Tracker " + name + " failed");

      notificationsBus.unregister(this);
      done.compareAndSet(false, true);
      this.not = not;

      if (future != null && !future.isCancelled()) {
        future.cancel(false);
      }

      if (mustStart != null && !mustStart.isCancelled()) {
        mustStart.cancel(false);
      }

      if (onFailure != null) {
        onFailure.run();
      }
    }
  }

  private void reset() {

    if (timedOut() || isDone()) {
      return;
    }

    logger.info("Tracker " + name + " reset");

    if (mustStart != null && !mustStart.isCancelled()) {
      mustStart.cancel(false);
    }

    if (future != null && !future.isCancelled()) {
      future.cancel(false);
    }

    if (!timer.isShutdown() && !timer.isTerminated()) {
      future = timer.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

  }

  public boolean complete() {
    return done.get();
  }

  public float percentComplete() {

    // Only if expected count has been set, do we compute percentage
    if (expect.get() != 0 && completed.get() != 0) {
      return ((expect.get() / completed.get()));
    }

    return 1;
  }

  public int completeCount() {
    return completed.get();
  }

  public int expectedCount() {
    return expect.get();
  }

  public int failedCount() {
    return failed.get();
  }

  public boolean timedOut() {
    return timedout.get();
  }

  public boolean isDone() {
    return done.get();
  }

  public ReactiveBoolean newBoolean() {
    return new ReactiveBoolean(this.notificationsBus);
  }

  @FunctionalInterface
  public interface Handler {

    public void handle(Notification n);
  }

  protected class Timeout implements Runnable {

    @Override
    public void run() {

      if (started.get() && !isDone() && !timedOut()) {
        logger.info("Tracker " + name + " timed out");
        notificationsBus.unregister(this);
        timedout.compareAndSet(false, true);

        if (done.get() && onCompletion != null) {
          onCompletion.handle(not);
        }

        if (onTimeout != null) {
          onTimeout.run();
        }
      }
    }
  }
}
