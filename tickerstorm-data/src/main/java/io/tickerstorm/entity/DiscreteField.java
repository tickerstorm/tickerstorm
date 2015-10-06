package io.tickerstorm.entity;

import java.time.Instant;

import com.google.common.base.MoreObjects;

public class DiscreteField extends BaseField<Integer> {

  public static final String TYPE = "discrete";

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
    DiscreteField other = (DiscreteField) obj;
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

  public String getInterval() {
    return interval;
  }

  public DiscreteField(String symbol, Instant timestamp, Integer quantity, String field,
      String source, String interval) {
    super(symbol, timestamp, field, source, quantity);
    this.interval = interval;
  }

  public DiscreteField(String symbol, Instant timestamp, Integer quantity, String field,
      String source) {
    super(symbol, timestamp, field, source, quantity);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("interval", this.interval).toString();
  }
}
