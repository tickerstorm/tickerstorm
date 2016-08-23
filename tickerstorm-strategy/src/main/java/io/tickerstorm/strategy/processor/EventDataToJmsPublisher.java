package io.tickerstorm.strategy.processor;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.data.eventbus.Destinations;

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


  @Subscribe
  public void onData(Serializable md) {

    logger.debug("Dispatching data event " + md);
    modelDataBus.post(md);

  }

}
