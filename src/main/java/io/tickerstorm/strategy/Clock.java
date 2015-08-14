package io.tickerstorm.strategy;

import java.time.Instant;


public interface Clock {

  public Instant now();

  public Instant update(Instant instant);

}
