package io.tickerstorm.common.entity;

import java.io.InputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.command.Command;

@SuppressWarnings("serial")
public class Session implements Serializable {

  public static final String SESSION_START = "session_start";

  public static final String SESSION_END = "session_end";
  private String stream;

  public final Map<String, Object> config = new HashMap<>();
  private EventBus commandsBus;
  private final AtomicBoolean live = new AtomicBoolean(false);
  Session(EventBus commandBus) {
    stream = UUID.randomUUID().toString();
    this.commandsBus = commandBus;
  }

  Session(String id, EventBus commandBus) {
    this.stream = id;
    this.commandsBus = commandBus;
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

  public void end() {
    live.set(false);
    Command marker = new Command(stream, Instant.now());
    marker.addMarker(SESSION_END);
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

  public boolean live() {
    return live.get();
  }

  public void start() {
    live.set(true);
    Command marker = new Command(stream, Instant.now());
    marker.addMarker(SESSION_START);
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

  public String stream() {
    return stream;
  }

}
