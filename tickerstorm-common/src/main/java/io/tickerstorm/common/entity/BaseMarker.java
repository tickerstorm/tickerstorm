package io.tickerstorm.common.entity;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("serial")
public class BaseMarker implements Marker {

  public static final String TYPE = "marker";
  public Integer expect = null;

  public Set<String> markers = new HashSet<>();

  public String id = UUID.randomUUID().toString();

  public BaseMarker(String id) {
    this.id = id;
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
