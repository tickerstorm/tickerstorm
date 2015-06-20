package io.tickerstorm.entity;

import java.io.Serializable;

public interface MarketData extends Event, Serializable {

  public String getSymbol();

}
