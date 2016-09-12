package io.tickerstorm.common.entity;

import java.time.Instant;

public interface Event {

  public Instant getTimestamp();
  public String getStream();
  public String getType();
  
}
