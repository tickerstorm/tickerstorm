package io.tickerstorm.data;

import java.util.HashSet;
import java.util.Set;

import org.springframework.stereotype.Component;

@Component
public class StooqHistoricalForexQuery implements QueryBuilder {

  private static final String HOST = "http://stooq.com/db/d/?b=";

  private String fileName = "_world_txt";
  private Set<String> securityTypes = new HashSet<String>();
  private String interval = "5";

  @Override
  public DataConverter converter() {
    return new StooqFileConverter();
  }

  public StooqHistoricalForexQuery withCurrencies() {
    this.fileName = "_world_txt";
    this.securityTypes.add("currencies");
    return this;
  }

  public StooqHistoricalForexQuery withBonds() {
    this.securityTypes.add("bonds");
    return this;
  }

  public StooqHistoricalForexQuery withCommodities() {
    this.securityTypes.add("commodities");
    return this;
  }

  public StooqHistoricalForexQuery withIndicies() {
    this.securityTypes.add("indicies");
    return this;
  }

  public StooqHistoricalForexQuery withUSData() {
    this.fileName = "_us_txt";
    return this;
  }

  public StooqHistoricalForexQuery int5min() {
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
