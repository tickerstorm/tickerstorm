package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.time.Instant;

import com.google.common.base.MoreObjects;

public class ContinousField extends BaseField<BigDecimal> {

  public static final String TYPE = "continous";

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

  @Override
  public String getFieldType() {
    return TYPE;
  }

  public ContinousField(String symbol, Instant timestamp, BigDecimal amount, String field,
      String source, String interval) {
    super(symbol, timestamp, field, source, amount);
    this.interval = interval;
  }

  public ContinousField(String symbol, Instant timestamp, BigDecimal amount, String field,
      String source) {
    super(symbol, timestamp, field, source, amount);
  }

  public String getInterval() {
    return interval;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("interval", this.interval).toString();
  }
}
