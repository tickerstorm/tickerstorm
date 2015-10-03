package io.tickerstorm.entity;

import java.io.Serializable;
import java.util.Set;

public interface MarketData extends Event, Serializable {

  public String getSymbol();

  public Set<Field<?>> getFields();

}
