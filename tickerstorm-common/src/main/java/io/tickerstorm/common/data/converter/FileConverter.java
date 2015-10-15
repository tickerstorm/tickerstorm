package io.tickerstorm.common.data.converter;

import io.tickerstorm.common.entity.MarketData;

public interface FileConverter {

  public MarketData[] convert(String line);

  public String provider();

  public Mode mode();

}
