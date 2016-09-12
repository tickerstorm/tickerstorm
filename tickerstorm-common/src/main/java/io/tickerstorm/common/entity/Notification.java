package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public class Notification implements Event, Marker, Stream, Serializable {

  public static final String TYPE = "notification";
  public String id = UUID.randomUUID().toString();
  public Set<String> markers = new HashSet<>();
  public String stream;

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public Map<String, String> properties = new HashMap<>();

  public Instant timestamp = Instant.now();
  public String source;

  public void addMarker(String marker) {
    markers.add(marker);
  }

  @Override
  public Set<String> getMarkers() {
    return markers;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getType() {
    return TYPE;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("source", source).add("timestamp", timestamp).add("markers", markers).add("type", TYPE)
        .toString();
  }



}
