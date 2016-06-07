package io.tickerstorm.client;

import java.io.Serializable;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.DataFeedQuery;
import net.engio.mbassy.bus.MBassador;


public abstract class BacktestRunnerClient implements ApplicationListener<ContextRefreshedEvent> {

  private static final Logger logger = LoggerFactory.getLogger(BacktestRunnerClient.class);
  private static final String clientName = "TickerStorm client";

  @Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS)
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private MBassador<Serializable> commandsBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationBus;

  @PostConstruct
  protected void init() throws Exception {
    commandsBus.subscribe(this);
    notificationBus.subscribe(this);
  }

  public void onStart() {

  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {
    onStart();
  }


}
