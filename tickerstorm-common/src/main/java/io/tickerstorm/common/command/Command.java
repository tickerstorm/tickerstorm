package io.tickerstorm.common.command;

import io.tickerstorm.common.reactive.Notification;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.Event;

@SuppressWarnings("serial")
public abstract class Command implements Marker, Event, Serializable {

  public String id = UUID.randomUUID().toString();
  public final Set<String> markers = new HashSet<>();
  public final Map<String, Object> config = new HashMap<>();
  public Instant timestamp = Instant.now();
  protected String stream;

  public void setStream(String stream) {
    this.stream = stream;
  }

  public final String type;

  public String getStream() {
    return stream;
  }

  public String id() {
    return id;
  }

  public Command(String stream, String type, String... marker) {
    this.stream = stream;
    this.markers.addAll(Lists.newArrayList(marker));
    this.type = type;
  }

  public Command(String stream, String type, Instant timestamp) {
    this.stream = stream;
    this.timestamp = timestamp;
    this.type = type;
  }

  public Command(String stream, String type) {
    this.stream = stream;
    this.type = type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((config == null) ? 0 : config.hashCode());
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
    Command other = (Command) obj;
    if (config == null) {
      if (other.config != null)
        return false;
    } else if (!config.equals(other.config))
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

  public Predicate<Notification> isCorrelated() {

    return n -> n.stream.equalsIgnoreCase(this.stream) && n.markers.containsAll(this.markers) && n.id.equals(this.id);

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
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

  @Override
  public String getType() {
    return type;
  }

  public abstract boolean isValid();

  protected boolean validate() {
    return (!StringUtils.isEmpty(this.stream) && !StringUtils.isEmpty(this.id) && this.timestamp != null);
  }

}
