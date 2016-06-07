package io.tickerstorm.common.entity;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.tickerstorm.common.data.eventbus.Destinations;
import net.engio.mbassy.bus.MBassador;

@Component
public class SessionFactory {

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private MBassador<Serializable> commandsBus;

  private SessionFactory() {}

  public Session newSession() {
    return new Session(commandsBus);
  }

  public Session newSession(String id) {
    return new Session(id, commandsBus);
  }

}


