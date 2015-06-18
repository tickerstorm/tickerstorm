package io.tickerstorm.entity;

import org.joda.time.DateTime;

public interface MarketData {

  public String getSymbol();
  public String getExchange();
  public DateTime getTimestamp();
  public String getSource();
  public String getType();

}
