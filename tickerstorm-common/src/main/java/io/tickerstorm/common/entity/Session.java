package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import net.engio.mbassy.bus.MBassador;

@SuppressWarnings("serial")
public class Session implements Serializable {

  public final String id;
  public final Map<String, String> config = new HashMap<>();
  private MBassador<Serializable> commandsBus;
  private final AtomicBoolean live = new AtomicBoolean(false);

  Session(MBassador<Serializable> commandBus) {
    id = UUID.randomUUID().toString();
    this.commandsBus = commandBus;
  };

  Session(String id, MBassador<Serializable> commandBus) {
    this.id = id;
    this.commandsBus = commandBus;
  }

  public void configure(Map<String, String> config) {
    this.config.putAll(config);
  }

  public void start() {
    live.set(true);
    Command marker = new Command(id, Instant.now());
    marker.addMarker(Markers.SESSION_START.toString());
    marker.config.putAll(this.config);
    commandsBus.publish(marker);
  }

  public boolean live(){
    return live.get();
  }
  
  public void end() {
    live.set(false);
    Command marker = new Command(id, Instant.now());
    marker.addMarker(Markers.SESSION_END.toString());
    marker.config.putAll(this.config);
    commandsBus.publish(marker);
  }

}
