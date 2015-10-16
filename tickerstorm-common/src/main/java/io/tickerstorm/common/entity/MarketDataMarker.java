package io.tickerstorm.common.entity;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("serial")
public class MarketDataMarker extends BaseMarketData implements Marker {

  public static final String TYPE = "marker";

  public Integer expect = null;

  public Set<String> markers = new HashSet<>();

  public final String id;

  public MarketDataMarker(String source, Instant timestamp) {
    super(source, timestamp);
    this.id = UUID.randomUUID().toString();
  }

  public MarketDataMarker(String symol, String source, Instant timestamp, String id) {
    super(symol, source, timestamp);
    this.id = id;
  }

  public void addMarker(String marker) {
    markers.add(marker);
  }

  public Set<String> getMarkers() {
    return markers;
  }

  @Override
  public String getType() {
    return TYPE;
  }

}
