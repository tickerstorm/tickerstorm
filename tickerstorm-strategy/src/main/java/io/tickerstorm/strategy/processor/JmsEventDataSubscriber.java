package io.tickerstorm.strategy.processor;

import java.io.Serializable;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.MarketData;
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

  @Autowired
  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  private MBassador<Map<String, Object>> retroModelDataBus;

  @Qualifier("retroEventBus")
  @Autowired
  private AsyncEventBus retroEventBus;

  @PostConstruct
  @Override
  protected void init() {
    super.init();
    realtimeBus.subscribe(this);
    commandsBus.subscribe(this);
    retroModelDataBus.subscribe(this);
  }

  @PreDestroy
  @Override
  protected void destroy() {
    super.destroy();
    realtimeBus.unsubscribe(this);
    commandsBus.unsubscribe(this);
    retroModelDataBus.unsubscribe(this);
  }

  @Handler
  public void onJmsMarketData(MarketData data) {
    publish(data);
  }

  @Handler
  public void onJmsCommand(Command comm) {
    publish(comm);
  }

  @Handler
  public void onJmsRetroModelData(Map<String, Object> row) {
    retroEventBus.post(row);
  }

}
