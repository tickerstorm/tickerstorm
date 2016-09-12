package io.tickerstorm.common.entity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.data.eventbus.Destinations;

@Component
public class SessionFactory {

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandsBus;

  private SessionFactory() {}

  public Session newSession() {
    return new Session(commandsBus);
  }

  public Session newSession(String stream) {
    return new Session(stream, commandsBus);
  }

}


