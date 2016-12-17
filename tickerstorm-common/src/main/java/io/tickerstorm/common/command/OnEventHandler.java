package io.tickerstorm.common.command;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

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
 * 
 * @author kkarski
 *
 */
public class OnEventHandler implements CompletionTracker {

  private final static Logger logger = LoggerFactory.getLogger(OnEventHandler.class);

  private final AtomicInteger completed = new AtomicInteger(0);
  private final AtomicInteger expect = new AtomicInteger(0);
  private final AtomicInteger failed = new AtomicInteger(0);

  private static final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
  private final EventBus notificationsBus;
  private final AtomicBoolean timedout = new AtomicBoolean(false);
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
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final Timeout task = new Timeout();
  private Notification not;
  private String name = "";

  @FunctionalInterface
  public interface Handler {
    public void handle(Notification n);
  }

  /**
   * 
   * @param startCondition Predicate to determine when the first correlated event has occurred to
   *        start the completion timer. I.e. the query start event was observed.
   * @param resetCondition Predicate to determine when to extend the timeout period by the delay
   *        amount. I.e. to wait out when all market data has streamed through the strategy
   * @param notificationsBus The notification bus over which all events will be received
   * @param delay The delay by which to extend the timeout each time we see a correlated event
   * @param timeout The amount of time to wait before the start condition must have been met.
   *        Prevents the tracker from waiting forever on an event which will never come
   * @param onCompletion The callback on completion
   * @param onTimeout The callback on timeout
   */
  protected OnEventHandler(EventBus notificationBus) {
    this.notificationsBus = notificationBus;
  }

  public static OnEventHandler newHandler(EventBus notificationBus) {
    OnEventHandler handler = new OnEventHandler(notificationBus);
    return handler;
  }

  public static OnEventHandler newHandler(EventBus notificationBus, String name) {
    OnEventHandler handler = new OnEventHandler(notificationBus);
    handler.name = name;
    return handler;
  }

  public OnEventHandler startCountDownOn(Predicate<Notification> startCondition) {
    this.timerStart = startCondition;
    return this;
  }

  public OnEventHandler extendTimeoutOn(Predicate<Notification> resetCondition, long delay) {
    this.resetCondition = resetCondition;
    this.delay = delay;
    return this;
  }

  public OnEventHandler completeWhen(Predicate<Notification> completionCondition) {
    this.completionCondition = completionCondition;
    return this;
  }

  public OnEventHandler failedWhen(Predicate<Notification> failureCondition) {
    this.failCondition = failureCondition;
    return this;
  }

  public OnEventHandler mustCompleteWithin(long delay) {
    this.delay = delay;
    return this;
  }

  public OnEventHandler whenTimedOut(Runnable onTimeout) {
    this.onTimeout = onTimeout;
    return this;
  }

  public OnEventHandler whenFailed(Runnable onTimeout) {
    this.onFailure = onTimeout;
    return this;
  }

  public OnEventHandler whenComplete(Handler onCompletion) {
    this.onCompletion = onCompletion;
    return this;
  }

  public void start() {
    this.notificationsBus.register(this);
    this.mustStart = timer.schedule(task, delay * 4, TimeUnit.MILLISECONDS);
  }

  @Subscribe
  public void onNotification(Notification not) {

    if (timerStart != null && timerStart.test(not) && future == null) {
      logger.debug("Tracker " + name + " started");
      reset();
    }

    if (resetCondition != null && resetCondition.test(not)) {

      if (not.is(Markers.START.toString()) && not.expect > 0) {

        expect.set(not.expect);

      } else if (not.is(Markers.SUCCESS.toString()) && not.expect > 0) {

        completed.getAndAdd(not.expect);

      } else if (not.is(Markers.FAILED.toString()) && not.expect > 0) {

        failed.getAndAdd(not.expect);

      }

      reset();

    }

    if (completionCondition != null && completionCondition.test(not)) {

      logger.debug("Tracker " + name + "completed");

      notificationsBus.unregister(this);
      done.compareAndSet(false, true);
      this.not = not;

      if (future != null && !future.isCancelled())
        future.cancel(false);

      if (mustStart != null && !mustStart.isCancelled())
        mustStart.cancel(false);

      if (onCompletion != null) {
        onCompletion.handle(not);
      }
    }

    if (failCondition != null && failCondition.test(not)) {

      logger.debug("Tracker " + name + "failed");

      notificationsBus.unregister(this);
      done.compareAndSet(false, true);
      this.not = not;

      if (future != null && !future.isCancelled())
        future.cancel(false);

      if (mustStart != null && !mustStart.isCancelled())
        mustStart.cancel(false);

      if (onFailure != null) {
        onFailure.run();
      }
    }
  }

  private void reset() {

    if (timedOut() || isDone())
      return;

    logger.debug("Tracker " + name + " reset");

    if (mustStart != null && !mustStart.isCancelled())
      mustStart.cancel(false);

    if (future != null && !future.isCancelled())
      future.cancel(false);

    if (!timer.isShutdown() && !timer.isTerminated())
      future = timer.schedule(task, delay, TimeUnit.MILLISECONDS);

  }

  public boolean complete() {
    return done.get();
  }

  public float percentComplete() {

    // Only if expected count has been set, do we compute percentage
    if (expect.get() != 0 && completed.get() != 0)
      return ((expect.get() / completed.get()));

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

  protected class Timeout implements Runnable {

    @Override
    public void run() {
      logger.debug("Tracker " + name + " timed out");
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
