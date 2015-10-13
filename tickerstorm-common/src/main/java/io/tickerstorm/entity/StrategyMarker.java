package io.tickerstorm.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class StrategyMarker implements Marker, Serializable {

  public Set<String> markers = new HashSet<>();
  public Map<String, String> config = new HashMap<>();

  @Override
  public Set<String> getMarkers() {
    return markers;
  }

  public void addMarker(String marker) {
    markers.add(marker);
  }

}
