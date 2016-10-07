package io.tickerstorm.service;

import java.io.Serializable;
import java.time.Instant;

@SuppressWarnings("serial")
public class HeartBeat implements Serializable {

  public final Instant timestamp = Instant.now();
  public final String service;

  public HeartBeat(String service) {
    this.service = service;
  }

  public String getService() {
    return service;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "Heartbeat from " + service;
  }

}
