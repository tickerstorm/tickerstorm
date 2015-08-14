package io.tickerstorm.data.query;

import io.tickerstorm.data.converter.DataConverter;

public interface QueryBuilder {

  public String build();

  public String provider();

  public DataConverter converter();

}
