package io.tickerstorm.common.entity;

import java.math.BigDecimal;
import java.time.Instant;

public class ContinousField extends BaseField<BigDecimal> {

  public static final String TYPE = "continous";

  @Override
  public String getFieldType() {
    return TYPE;
  }

  public ContinousField(String symbol, Instant timestamp, BigDecimal amount, String field, String source, String interval) {
    super(symbol, timestamp, field, source, amount, interval);
  }
  
  public ContinousField(String symbol, Instant timestamp, BigDecimal amount, String field, String source) {
    super(symbol, timestamp, field, source, amount, null);
  }

  public static Field<BigDecimal> deserialize(String value) {

    String vals = parseValue(value);
    String[] fields = parseFields(value);
    ContinousField field = new ContinousField(fields[4], Instant.parse(fields[5]), new BigDecimal(vals), fields[3], fields[1], fields[2]);
    return field;

  }
}
