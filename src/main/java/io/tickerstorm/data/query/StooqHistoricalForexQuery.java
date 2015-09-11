package io.tickerstorm.data.query;

import io.tickerstorm.data.converter.DataConverter;
import io.tickerstorm.data.converter.StooqFileConverter;

import java.util.HashSet;
import java.util.Set;

import org.springframework.stereotype.Component;

@Component
public class StooqHistoricalForexQuery implements QueryBuilder {

  private static final String HOST = "http://stooq.com/db/h/";

  private String fileName = "_world_txt.zip";
  private Set<String> securityTypes = new HashSet<String>();
  private String interval = "5";

  @Override
  public DataConverter converter() {
    return new StooqFileConverter();
  }

  public StooqHistoricalForexQuery currencies() {
    this.fileName = "_world_txt.zip";
    this.securityTypes.add("currencies");
    return this;
  }

  public StooqHistoricalForexQuery bonds() {
    this.securityTypes.add("bonds");
    return this;
  }

  public StooqHistoricalForexQuery commodities() {
    this.securityTypes.add("commodities");
    return this;
  }

  public StooqHistoricalForexQuery indicies() {
    this.securityTypes.add("indicies");
    return this;
  }

  public StooqHistoricalForexQuery etfs() {
    this.securityTypes.add("nasdaq etfs");
    this.securityTypes.add("nyse etfs");
    this.securityTypes.add("nysqmkt etfs");
    return this;
  }

  public StooqHistoricalForexQuery stocks() {
    this.securityTypes.add("nasdaq stocks");
    this.securityTypes.add("nyse stocks");
    this.securityTypes.add("nysqmkt stocks");
    return this;
  }

  public StooqHistoricalForexQuery forUS() {
    this.fileName = "_us_txt.zip";
    return this;
  }

  public StooqHistoricalForexQuery forWorld() {
    this.fileName = "_world_txt.zip";
    return this;
  }

  public StooqHistoricalForexQuery min5() {
    this.interval = "5";
    return this;
  }

  public StooqHistoricalForexQuery hourly() {
    this.interval = "h";
    return this;
  }

  public StooqHistoricalForexQuery daily() {
    this.interval = "d";
    return this;
  }

  @Override
  public String build() {
    return HOST + interval + fileName;
  }

  @Override
  public String provider() {
    return "Stooq";
  }

}
