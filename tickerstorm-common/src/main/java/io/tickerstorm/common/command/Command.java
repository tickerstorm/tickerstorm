package io.tickerstorm.common.command;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.Event;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.Stream;

@SuppressWarnings("serial")
public class Command implements Marker, Event, Stream, Serializable {

  public String id = UUID.randomUUID().toString();
  public Set<String> markers = new HashSet<>();
  public Map<String, Object> config = new HashMap<>();
  public Instant timestamp = Instant.now();
  public static final String TYPE = "command";
  public String stream;

  public String getStream() {
    return stream;
  }

  public String getId() {
    return id;
  }

  public Command(String stream) {
    this.stream = stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public Command(String stream, String... marker) {
    this.stream = stream;
    this.markers.addAll(Lists.newArrayList(marker));
  }

  public Command(String stream, Instant timestamp) {
    this.stream = stream;
    this.timestamp = timestamp;
  }

  @Override
  public Set<String> getMarkers() {
    return markers;
  }

  public void addMarker(String marker) {
    markers.add(marker);
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("stream", stream).add("timestamp", timestamp).add("markers", markers).add("type", TYPE)
        .toString();
  }

}
