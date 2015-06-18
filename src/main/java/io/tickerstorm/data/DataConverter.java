package io.tickerstorm.data;

import io.tickerstorm.entity.MarketData;

public interface DataConverter {

  public MarketData[] convert(String line);

  public String provider();

}
