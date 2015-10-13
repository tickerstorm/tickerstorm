package io.tickerstorm.data.converter;

import java.util.Set;

import io.tickerstorm.data.query.DataQuery;
import io.tickerstorm.entity.MarketData;

public interface DataConverter {

  public Set<String> namespaces();

  public MarketData[] convert(String line, DataQuery query);

  public String provider();

  public Mode mode();

}
