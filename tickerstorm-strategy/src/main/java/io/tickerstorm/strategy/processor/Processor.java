package io.tickerstorm.strategy.processor;

import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Notification;

public interface Processor {

  void onNotification(Notification notification) throws Exception;

  void onCommand(Command command) throws Exception;



}
