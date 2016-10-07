package io.tickerstorm.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.eventbus.Destinations;

@Component
public class SessionFactory {

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandsBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  private SessionFactory() {}

  public Session newSession() {
    return new Session(commandsBus, notificationBus);
  }

  public Session newSession(String stream) {
    return new Session(stream, commandsBus, notificationBus);
  }

}


