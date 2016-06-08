package io.tickerstorm.common.entity;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("serial")
public class BaseMarker implements Marker, Stream {

  public static final String TYPE = "marker";
  public Integer expect = null;
  public String stream;

  public Set<String> markers = new HashSet<>();

  public final String id;

  public BaseMarker(String id, String stream) {
    this.id = id;
    this.stream = stream;
  }

  public BaseMarker(String stream) {
    this.id = UUID.randomUUID().toString();
    this.stream = stream;
  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  @Override
  public Set<String> getMarkers() {
    return markers;
  }

  public void addMarker(String marker) {
    markers.add(marker);
  }

  public String getType() {
    return TYPE;
  }
}
