package io.tickerstorm.strategy.processor;

import java.io.Serializable;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Notification;
import net.engio.mbassy.bus.MBassador;

/**
 * This implementation bridges the AsyncEvent bus used by the event processors and MBassador even
 * bus used to dispatch messages to JMSTemplate over the jms event bridge
 * 
 * @author kkarski
 *
 */
@Component
public class EventDataToJmsPublisher extends BaseEventProcessor {

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private AsyncEventBus modelDataBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationsBus;

  @Subscribe
  public void onMarketData(MarketData md) {

    logger.debug("Dispatching processed market data event " + md);

    Map<String, Object> out = Maps.newHashMap();
    out.put(Field.Name.MARKETDATA.field(), md);
    modelDataBus.post(out);

  }

  @Subscribe
  public void onField(Field<?> field) {

    logger.debug("Dispatching processed field " + field);

    Map<String, Object> out = Maps.newHashMap();
    out.put(field.getName(), field);
    modelDataBus.post(out);

  }

  @Subscribe
  @Override
  public void onNotification(Notification notification) throws Exception {

    super.onNotification(notification);
    notificationsBus.publish(notification);
  }
}
