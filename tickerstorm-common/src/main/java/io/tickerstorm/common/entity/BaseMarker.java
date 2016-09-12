package io.tickerstorm.common.entity;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("serial")
public class BaseMarker implements Marker, Stream {

  public static final String TYPE = "marker";
  public Integer expect = null;
  public String stream;
  public Instant eventTime = Instant.now();
  
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((eventTime == null) ? 0 : eventTime.hashCode());
    result = prime * result + ((expect == null) ? 0 : expect.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((markers == null) ? 0 : markers.hashCode());
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BaseMarker other = (BaseMarker) obj;
    if (eventTime == null) {
      if (other.eventTime != null)
        return false;
    } else if (!eventTime.equals(other.eventTime))
      return false;
    if (expect == null) {
      if (other.expect != null)
        return false;
    } else if (!expect.equals(other.expect))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (markers == null) {
      if (other.markers != null)
        return false;
    } else if (!markers.equals(other.markers))
      return false;
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
      return false;
    return true;
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
