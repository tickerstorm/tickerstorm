package io.tickerstorm.common.data.query;

import java.util.Map;

public interface DataQuery {

  public String namespace();

  public String build();
  
  public Map<String, String> headers();

  public String provider();

  public String getSymbol();

  public String getInterval();

}
