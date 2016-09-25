package io.tickerstorm.service;

import java.io.Serializable;

@SuppressWarnings("serial")
public class HeartBeat implements Serializable {

  public final long timestamp = System.currentTimeMillis();
  public final String service;

  public HeartBeat(String service) {
    this.service = service;
  }

  public String getService() {
    return service;
  }

  public long getTimestamp() {
    return timestamp;
  }

}
