package io.tickerstorm.strategy.processor.eventbus;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.processor.BaseEventProcessor;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@Component
public class JmsEventDataSubscriber extends BaseEventProcessor {

  @Autowired
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  private MBassador<MarketData> realtimeBus;

  @Autowired
  @Qualifier(Destinations.COMMANDS_BUS)
  private MBassador<Serializable> commandsBus;

  @PostConstruct
  @Override
  protected void init() {
    super.init();
    realtimeBus.subscribe(this);
    commandsBus.subscribe(this);
  }

  @PreDestroy
  @Override
  protected void destroy() {
    super.destroy();
    realtimeBus.unsubscribe(this);
    commandsBus.unsubscribe(this);
  }

  @Handler
  public void onJmsMarketData(MarketData data) {
    publish(data);
  }

  @Handler
  public void onJmsCommand(Command comm) {
    publish(comm);
  }
}
