package io.tickerstorm.service;

import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.eventbus.Destinations;

public class HeartBeatGenerator {

  private static final Logger logger = LoggerFactory.getLogger(HeartBeatGenerator.class);

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notifications;

  private String serviceName;
  private long interval = 5000;

  private Timer timer = new Timer("heartbeat", true);

  public HeartBeatGenerator(String serviceName, long interval) {
    this.serviceName = serviceName;
    this.interval = interval;
  }

  @EventListener
  public void handleInitialized(ContextRefreshedEvent e) {
    timer.scheduleAtFixedRate(new HeartBeatTask(), 0, interval);
  }

  @PreDestroy
  public void destroy() {
    timer.purge();
    timer.cancel();
  }

  private class HeartBeatTask extends TimerTask {

    @Override
    public void run() {
      logger.trace(serviceName + " sending heartbeat");
      notifications.post(new HeartBeat(serviceName));
    }

  }

}
