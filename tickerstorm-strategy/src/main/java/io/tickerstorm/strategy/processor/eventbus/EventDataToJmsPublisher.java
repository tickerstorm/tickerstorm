package io.tickerstorm.strategy.processor.eventbus;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;

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
  private EventBus modelDataBus;

  @Subscribe
  public void onData(Field<?> field) {
    logger.trace("Dispatching data event " + field);
    modelDataBus.post(field);
  }

  @Subscribe
  public void onData(Collection<Field<?>> fs) {
    logger.trace("Dispatching collection of fields " + fs);
    modelDataBus.post(fs);
  }

}
