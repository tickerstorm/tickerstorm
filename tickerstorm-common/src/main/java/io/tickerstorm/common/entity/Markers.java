package io.tickerstorm.common.entity;

public enum Markers {

  QUERY("query"),

  MARKET_DATA("market_data"),

  MODEL_DATA("model_data"),

  SAVED("saved"),

  CREATED("created"),

  DELETED("deleted"),

  END("end"),

  START("start"),
  
  FAILED("failed");


  public String marker;

  private Markers(String marker) {
    this.marker = marker;
  }

  @Override
  public String toString() {
    return marker;
  }

  public static boolean is(Marker marker, Markers m) {

    if (marker != null && m != null)
      return (marker.getMarkers().contains(m.toString()));

    return false;
  }
}
