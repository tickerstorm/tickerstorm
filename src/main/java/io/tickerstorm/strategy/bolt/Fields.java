package io.tickerstorm.strategy.bolt;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;


public enum Fields {

  MARKETDATA("marketdata"), CANDEL("candel"), QUOTE("quote"), TICK("tick"), AVE("ave"), MA("ma"), NOW(
      "now");

  private final String fieldName;

  Fields(String fieldName) {
    this.fieldName = fieldName;
  }

  public String fieldName() {
    return fieldName;
  }

  public static Set<String> marketdataFields() {
    return Collections.unmodifiableSet(Sets.newHashSet(
        io.tickerstorm.strategy.bolt.Fields.MARKETDATA.fieldName(),
        io.tickerstorm.strategy.bolt.Fields.CANDEL.fieldName(),
        io.tickerstorm.strategy.bolt.Fields.QUOTE.fieldName(),
        io.tickerstorm.strategy.bolt.Fields.TICK.fieldName()));
  }
}
