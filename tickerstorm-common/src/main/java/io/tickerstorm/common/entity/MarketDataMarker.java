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

  public String id = UUID.randomUUID().toString();;

  public MarketDataMarker(String source, Instant timestamp) {
    super(source, timestamp);
  }

  public MarketDataMarker(String symol, String source) {
    super(symol, source, Instant.now());
  }

  public MarketDataMarker(String symol, String source, String marker) {
    super(symol, source, Instant.now());
    markers.add(marker);
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
