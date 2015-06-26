package io.tickerstorm.data;

import io.tickerstorm.entity.MarketData;

public interface DataConverter {

  public enum Mode {
    doc, line, file;
  }
  
  public MarketData[] convert(String line);
  
  public String provider();
  public Mode mode();

}
