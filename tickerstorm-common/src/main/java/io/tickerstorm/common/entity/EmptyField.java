package io.tickerstorm.common.entity;

import java.time.Instant;

public class EmptyField extends BaseField<String> {

  public static final String TYPE = "empty";

  public EmptyField(String symbol, Instant timestamp, String field, String source, String interval) {
    super(symbol, timestamp, field, source, null, interval);
  }

  public EmptyField(String symbol, Instant timestamp, String field, String source) {
    super(symbol, timestamp, field, source, null, null);
  }

  @Override
  public String getFieldType() {
    return TYPE;
  }

  public static EmptyField deserialize(String value) {
    
    String[] fields = parseFields(value);
    EmptyField field = new EmptyField(fields[4], Instant.parse(fields[5]), fields[3], fields[1], fields[2]);
    return field;

  }


}
