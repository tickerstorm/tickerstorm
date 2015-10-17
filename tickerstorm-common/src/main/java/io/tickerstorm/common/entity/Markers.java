package io.tickerstorm.common.entity;

public enum Markers {

  /**
   * First market data of the day
   */
  MARKET_OPEN("market_open"),

  /**
   * Last market data of the day
   */
  MARKET_CLOSE("market_close"),

  /**
   * Last market data of this query
   */
  QUERY_END("query_end"),

  /**
   * First market data of this query
   */
  QUERY_START("query_start"),

  /**
   * Backtest session end
   */
  SESSION_END("session_end"),

  /**
   * Backtests session start
   */
  SESSION_START("session_start"),
  /**
   * Backtest session end
   */
  CSV_CREATED("csv_created"),;


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
