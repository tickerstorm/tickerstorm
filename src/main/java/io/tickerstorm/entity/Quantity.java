package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.time.Instant;

public class Quantity extends NumericField {

  public Quantity(String symbol, Instant timestamp, BigDecimal quantity, String field, String interval) {
    super(symbol, timestamp, quantity, field, interval);
  }

  public Quantity(String symbol, Instant timestamp, BigDecimal quantity, String field) {
    super(symbol, timestamp, quantity, field);
  }

}
