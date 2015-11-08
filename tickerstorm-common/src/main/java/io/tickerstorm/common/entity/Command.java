package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class Command implements Marker, Event, Serializable {

  public String id = UUID.randomUUID().toString();
  public Set<String> markers = new HashSet<>();
  public Map<String, String> config = new HashMap<>();
  public Instant timestamp = Instant.now();
  public String source;
  public static final String TYPE = "command";

  public String getId() {
    return id;
  }

  public Command(String source) {
    this.source = source;
  }

  public Command(String source, String... marker) {
    this.source = source;
    this.markers.addAll(Lists.newArrayList(marker));
  }

  public Command(String source, Instant timestamp) {
    this.source = source;
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
  public String getSource() {
    return source;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("source", source).add("timestamp", timestamp).add("markers", markers).add("type", TYPE)
        .toString();
  }

}
