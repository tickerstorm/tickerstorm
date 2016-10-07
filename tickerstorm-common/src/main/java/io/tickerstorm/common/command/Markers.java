package io.tickerstorm.common.command;

public enum Markers {

  QUERY("query"),

  MARKET_DATA("market_data"),

  MODEL_DATA("model_data"),
  
  SESSION("session"),

  FILE("file"),

  INGEST("ingested"),

  SAVE("save"),

  CREATED("created"),

  DELETE("deleted"),

  END("end"),

  START("start"),
  
  EXPORT("export"),
  
  CSV("csv"),
  
  LOCATION("location"),

  FAILED("failed"),
  
  SUCCESS("success");


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
