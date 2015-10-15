package io.tickerstorm.common.data.converter;

import java.util.Set;

import io.tickerstorm.common.data.query.DataQuery;
import io.tickerstorm.common.entity.MarketData;

public interface DataConverter {

  public Set<String> namespaces();

  public MarketData[] convert(String line, DataQuery query);

  public String provider();

  public Mode mode();

}
