package io.tickerstorm.entity;

import java.time.Instant;

public class DiscreteField extends BaseField<Integer> {

  public String interval;

  public String getInterval() {
    return interval;
  }

  public DiscreteField(String symbol, Instant timestamp, Integer quantity, String field,
      String interval) {
    super(symbol, timestamp, field, interval, quantity);
    this.interval = interval;
  }

  public DiscreteField(String symbol, Instant timestamp, Integer quantity, String field) {
    super(symbol, timestamp, field, quantity);
  }

}
