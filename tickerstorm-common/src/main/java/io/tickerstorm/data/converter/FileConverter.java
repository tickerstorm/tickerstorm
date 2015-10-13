package io.tickerstorm.data.converter;

import io.tickerstorm.entity.MarketData;

public interface FileConverter {

  public MarketData[] convert(String line);

  public String provider();

  public Mode mode();

}
