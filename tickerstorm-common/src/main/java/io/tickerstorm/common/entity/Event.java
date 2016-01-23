package io.tickerstorm.common.entity;

import java.time.Instant;

public interface Event {

  public Instant getTimestamp();
  public String getSource();
  public String getType();
  
}
