package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.time.Instant;

public class ContinousField extends BaseField<BigDecimal> {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContinousField other = (ContinousField) obj;
    if (interval == null) {
      if (other.interval != null)
        return false;
    } else if (!interval.equals(other.interval))
      return false;
    return true;
  }

  public String interval;

  public ContinousField(String symbol, Instant timestamp, String currency, BigDecimal amount,
      String field, String interval) {
    super(symbol, timestamp, currency, field, amount);
    this.interval = interval;
  }

  public ContinousField(String symbol, Instant timestamp, String currency, BigDecimal amount,
      String field) {
    super(symbol, timestamp, currency, field, amount);
  }

  public String getInterval() {
    return interval;
  }
}
