package io.tickerstorm.client;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.data.eventbus.Destinations;


public abstract class BacktestRunnerClient implements ApplicationListener<ContextRefreshedEvent> {

  private static final Logger logger = LoggerFactory.getLogger(BacktestRunnerClient.class);
  private static final String clientName = "TickerStorm client";

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandsBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @PostConstruct
  protected void init() throws Exception {
    commandsBus.register(this);
    notificationBus.register(this);
  }

  public void onStart() {

  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {
    onStart();
  }


}
