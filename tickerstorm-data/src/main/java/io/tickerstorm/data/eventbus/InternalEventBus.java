package io.tickerstorm.data.eventbus;

import java.io.Serializable;

public interface InternalEventBus {

  public void async(Serializable event, String destiation);

  public void sync(Serializable event, String destiation);

}
