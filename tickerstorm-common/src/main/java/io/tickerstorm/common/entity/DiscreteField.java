package io.tickerstorm.common.entity;

import java.time.Instant;

import com.google.common.base.MoreObjects;

public class DiscreteField extends BaseField<Integer> {

  public static final String TYPE = "discrete";

  @Override
  public String getFieldType() {
    return TYPE;
  }

  public DiscreteField(String symbol, Instant timestamp, Integer quantity, String field, String source, String interval) {
    super(symbol, timestamp, field, source, quantity, interval);
  }
  
  public DiscreteField(String symbol, Instant timestamp, Integer quantity, String field, String source) {
    super(symbol, timestamp, field, source, quantity, null);
  }
  
  public static DiscreteField deserialize(String value) {

    String vals = parseValue(value);
    String[] fields = parseFields(value);
    DiscreteField field = new DiscreteField(fields[4], Instant.parse(fields[5]), new Integer(vals), fields[3], fields[1], fields[2]);    
    return field;

  }
}
