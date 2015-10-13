package io.tickerstorm.entity;

import java.time.Instant;

public interface Event {

  public Instant getTimestamp();
  public String getSource();
  public String getType();

}
