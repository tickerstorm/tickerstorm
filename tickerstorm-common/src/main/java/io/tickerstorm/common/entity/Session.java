package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.command.Command;

@SuppressWarnings("serial")
public class Session implements Serializable {

  public static final String SESSION_START = "session_start";
  public static final String SESSION_END = "session_end";

  public final String stream;
  public final Map<String, String> config = new HashMap<>();
  private EventBus commandsBus;
  private final AtomicBoolean live = new AtomicBoolean(false);

  Session(EventBus commandBus) {
    stream = UUID.randomUUID().toString();
    this.commandsBus = commandBus;
  };

  Session(String id, EventBus commandBus) {
    this.stream = id;
    this.commandsBus = commandBus;
  }

  public void configure(Map<String, String> config) {
    this.config.putAll(config);
  }

  public void start() {
    live.set(true);
    Command marker = new Command(stream, Instant.now());
    marker.addMarker(SESSION_START);
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

  public boolean live() {
    return live.get();
  }

  public void end() {
    live.set(false);
    Command marker = new Command(stream, Instant.now());
    marker.addMarker(SESSION_END);
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

}
