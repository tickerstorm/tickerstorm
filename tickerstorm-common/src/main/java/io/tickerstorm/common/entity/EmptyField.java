package io.tickerstorm.common.entity;

import java.time.Instant;

public class EmptyField extends BaseField<String> {

  public static final String TYPE = "empty";

  public EmptyField(String symbol, Instant timestamp, String field, String source) {
    super(symbol, timestamp, field, source, "");
  }

  public EmptyField(String symbol, Instant timestamp, String field, String source, String interval) {
    super(symbol, timestamp, field, source, "");
    this.interval = interval;
  }

  @Override
  public String getInterval() {
    return interval;
  }

  @Override
  public String getFieldType() {
    return TYPE;
  }
}
