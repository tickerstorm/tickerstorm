package io.tickerstorm.common;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Markers;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.DefaultResourceLoader;
import org.yaml.snakeyaml.Yaml;

@SuppressWarnings("serial")
public class Session implements Serializable {

  public final Map<String, Object> config = new HashMap<>();
  private final AtomicBoolean live = new AtomicBoolean(false);
  private String stream;
  private EventBus commandsBus;
  private EventBus notificationsBus;

  Session(EventBus commandBus, EventBus notificationBus) {
    stream = UUID.randomUUID().toString();
    this.commandsBus = commandBus;
    this.notificationsBus = notificationBus;
  }

  Session(String id, EventBus commandBus, EventBus notificationBus) {
    this.stream = id;
    this.stream = this.stream.toLowerCase();
    this.commandsBus = commandBus;
    this.notificationsBus = notificationBus;
  }

  public void configure(InputStream stream) {
    Yaml yaml = new Yaml();
    Map<String, Object> content = (Map) yaml.load(stream);
    this.stream = (String) content.get("stream");
    assert !StringUtils.isEmpty(this.stream) : "A stream name must be provided";
    this.stream = this.stream.toLowerCase();
    this.config.putAll(content);
  }

  public void configure(InputStream input, String stream) {
    Yaml yaml = new Yaml();
    Map<String, Object> content = (Map) yaml.load(input);
    this.stream = stream;
    assert !StringUtils.isEmpty(this.stream) : "A stream name must be provided";
    this.stream = this.stream.toLowerCase();
    this.config.putAll(content);
  }

  public void configure(URI uri, String stream) throws IOException {
    this.configure(new DefaultResourceLoader().getResource(uri.toString()).getInputStream(), stream);
  }

  public void configure(Map<String, Object> config) {
    this.config.putAll(config);
  }

  ;

  public void configure(String yamlContent) {
    Yaml yaml = new Yaml();
    Map<String, Object> content = (Map) yaml.load(yamlContent);
    this.stream = (String) content.get("stream");
    assert !StringUtils.isEmpty(this.stream) : "A stream name must be provided";
    this.stream = this.stream.toLowerCase();
    this.config.putAll(content);
  }

  public void end() {
    live.set(false);
    Command marker = new Command(stream, "session.end");
    marker.addMarker(Markers.SESSION.toString());
    marker.addMarker(Markers.END.toString());
    marker.config.putAll(this.config);
    commandsBus.post(marker);
  }

  public void execute(Command comm) {
    comm.setStream(this.stream);
    assert comm.isValid() : "Command isn't valid";
    commandsBus.post(comm);
  }

  public EventBus getCommandsBus() {
    return commandsBus;
  }

  public EventBus getNotificationsBus() {
    return notificationsBus;
  }

  public boolean live() {
    return live.get();
  }

  public void start() {
    live.set(true);
    Command marker = new Command(stream, "session.start");
    marker.addMarker(Markers.SESSION.toString());
    marker.addMarker(Markers.START.toString());
    marker.config.putAll(this.config);
    commandsBus.post(marker);

    //TODO: Ensure session.start() waits for confirmation from all system resources on configuration acknowledgement before continue. 
    //This is breaking end to end integration tests becasue streaming data reaches strategy before processors are configured
  }

  public String stream() {
    return stream;
  }

}
