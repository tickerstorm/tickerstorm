package io.tickerstorm.common;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Trigger;

@SuppressWarnings("serial")
public class Session implements Serializable {

  private String stream;
  public final Map<String, Object> config = new HashMap<>();
  private EventBus commandsBus;
  private EventBus notificationsBus;
  private final AtomicBoolean live = new AtomicBoolean(false);

  Session(EventBus commandBus, EventBus notificationBus) {
    stream = UUID.randomUUID().toString();
    this.commandsBus = commandBus;
    this.notificationsBus = notificationBus;
  }

  Session(String id, EventBus commandBus, EventBus notificationBus) {
    this.stream = id;
    this.commandsBus = commandBus;
    this.notificationsBus = notificationBus;
  };

  public void configure(InputStream stream) {
    Yaml yaml = new Yaml();
    Map<String, Object> content = (Map) yaml.load(stream);
    this.stream = (String) content.get("stream");
    assert !StringUtils.isEmpty(this.stream) : "A stream name must be provided";
    this.config.putAll(content);
  }

  public void configure(Map<String, Object> config) {
    this.config.putAll(config);
  }

  public void configure(String yamlContent) {
    Yaml yaml = new Yaml();
    Map<String, Object> content = (Map) yaml.load(yamlContent);
    this.stream = (String) content.get("stream");
    assert !StringUtils.isEmpty(this.stream) : "A stream name must be provided";
    this.config.putAll(content);
  }

  public void execute(Command comm) {
    comm.setStream(this.stream);
    assert comm.isValid() : "Command isn't valid";
    commandsBus.post(comm);
  }

  public void end() {
    live.set(false);
    Trigger marker = new Trigger(stream, "session.end");
    marker.addMarker(Markers.SESSION.toString());
    marker.addMarker(Markers.END.toString());
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

  public boolean live() {
    return live.get();
  }

  public void start() {
    live.set(true);
    Trigger marker = new Trigger(stream, "session.start");
    marker.addMarker(Markers.SESSION.toString());
    marker.addMarker(Markers.START.toString());
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

  public String stream() {
    return stream;
  }

}
