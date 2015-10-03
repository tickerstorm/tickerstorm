package io.tickerstorm.entity;

import java.time.Instant;


public class CategoricalField extends BaseField<String> {

  public static final String TYPE = "categorical";

  public CategoricalField(String symbol, Instant timestamp, String value, String field,
      String source) {
    super(symbol, timestamp, field, source, value);
  }

  @Override
  public String getInterval() {
    return "";
  }

  @Override
  public String getFieldType() {
    return TYPE;
  }

}
