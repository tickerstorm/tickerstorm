package io.tickerstorm.common.model;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;


public enum Fields {

  MARKETDATA("marketdata"), CANDEL("candel"), QUOTE("quote"), TICK("tick"), AVE("ave"), SMA("ma"), NOW("now"), MARKER("marker"), MODEL_NAME(
      "modelname"), FEATURES("features");

  private final String fieldName;

  Fields(String fieldName) {
    this.fieldName = fieldName;
  }

  public String fieldName() {
    return fieldName;
  }

  @Override
  public String toString() {
    return fieldName;
  }

  public static Set<String> marketdataFields() {
    return Collections.unmodifiableSet(
        Sets.newHashSet(io.tickerstorm.common.model.Fields.MARKETDATA.fieldName(), io.tickerstorm.common.model.Fields.NOW.fieldName()));
  }
}
