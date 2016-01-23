package io.tickerstorm.common.entity;

import java.time.Instant;

@SuppressWarnings("serial")
public class CategoricalField extends BaseField<String> {

  public static final String TYPE = "categorical";

  public CategoricalField(String symbol, Instant timestamp, String value, String field, String source, String interval) {
    super(symbol, timestamp, field, source, value, interval);
  }

  public CategoricalField(String symbol, Instant timestamp, String value, String field, String source) {
    super(symbol, timestamp, field, source, value, null);
  }

  @Override
  public String getFieldType() {
    return TYPE;
  }

  public static Field<String> deserialize(String value) {
    
    String vals = parseValue(value);
    String[] fields = parseFields(value);
    CategoricalField field = new CategoricalField(fields[4], Instant.parse(fields[5]), vals, fields[3], fields[1], fields[2]);    
    return field;
  }

}
