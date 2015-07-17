package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.time.Instant;

public class Amount extends NumericField {
  
  public Amount(String symbol, Instant timestamp, String currency, BigDecimal amount, String field, String interval) {
    super(symbol, timestamp, currency, amount, field, interval);
  }
  
  public Amount(String symbol, Instant timestamp, String currency, BigDecimal amount, String field) {
    super(symbol, timestamp, currency, amount, field);
  }

}
