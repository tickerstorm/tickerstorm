package io.tickerstorm.entity;

import java.time.Instant;


public class CategoricalField extends BaseField<String> {

  public CategoricalField(String symbol, Instant timestamp, String field, String value) {
    super(symbol, timestamp, field, value);
  }



}
