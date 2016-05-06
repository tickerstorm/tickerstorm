package io.tickerstorm.data.query;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tickerstorm.common.data.query.DataQuery;
import io.tickerstorm.common.entity.Candle;

public class GoogleDataQuery implements QueryBuilder, DataQuery {

  public String getSymbol() {
    return symbol;
  }

  @Override
  public String namespace() {
    return HOST;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  private static final Logger logger = LoggerFactory.getLogger(GoogleDataQuery.class);

  public static final String HOST = "http://www.google.com/finance/getprices";
  public static final Integer MIN_1 = 60;

  private String symbol;
  private Integer period = 15;
  private String[] fields = new String[] {"d", "c", "h", "l", "o", "v"};

  public GoogleDataQuery(String symbol) {
    this.symbol = symbol;
  }

  @Override
  public String getInterval() {
    return Candle.MIN_1_INTERVAL;
  }

  public String build() {
    return HOST + "?q=" + symbol + "&i=" + MIN_1 + "&p=" + period + "d&f=" + String.join(",", fields);
  }

  public String type() {
    return Candle.TYPE;
  }

  @Override
  public String provider() {
    return "Google";
  }

  public GoogleDataQuery days(int i) {

    if (i < 16 && i > 0)
      period = i;

    return this;
  }

  @Override
  public Map<String, String> headers() {
    // TODO Auto-generated method stub
    return null;
  }



}
