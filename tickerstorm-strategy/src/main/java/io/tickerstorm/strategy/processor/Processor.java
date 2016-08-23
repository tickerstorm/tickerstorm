package io.tickerstorm.strategy.processor;

import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Notification;

public interface Processor {

  void onNotification(Notification notification) throws Exception;

  void onCommand(Command command) throws Exception;

  

}
