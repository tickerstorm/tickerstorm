package io.tickerstorm.data;

public interface QueryBuilder {

  public String build();

  public String provider();

  public DataConverter converter();

}
