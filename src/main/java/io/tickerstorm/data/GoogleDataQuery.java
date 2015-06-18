package io.tickerstorm.data;

import io.tickerstorm.entity.Candle;

import java.util.Arrays;

public class GoogleDataQuery implements QueryBuilder {

  public static final String HOST = "http://www.google.com/finance/getprices";
  public static final Integer MIN_5 = 300;
  public static final Integer MIN_1 = 60;
  public static final Integer MIN_3 = 180;
  public static final String LOW = "l";
  public static final String HIGH = "h";
  public static final String OPEN = "o";
  public static final String VOLUME = "v";
  public static final String CLOSE = "c";
  public static final String D = "d";

  private String symbol;
  private String exchange = "NASDAQ";
  private Integer period = MIN_5;
  private String[] fields = new String[] { "d", "c", "v", "o", "h", "l" };
  private DataConverter converter;

  public GoogleDataQuery(String symbol, DataConverter converter) {
    this.symbol = symbol;
    this.converter = converter;
    
  }

  public GoogleDataQuery withSymbol(String symbol) {
    this.symbol = symbol;
    return this;
  }

  public GoogleDataQuery withExchange(String exchange) {
    this.exchange = exchange;
    return this;
  }

  public GoogleDataQuery withPeriod(Integer period) {
    this.period = period;
    return this;
  }

  public GoogleDataQuery withFields(String... fields) {
    this.fields = Arrays.asList(fields).toArray(new String[] {});
    return this;
  }

  public String build() {
    return HOST + "?q" + symbol + "&x=" + exchange + "&p=" + period + "&f=" + String.join(",", fields);
  }

  public String type() {
    return Candle.TYPE;
  }

  @Override
  public String provider() {
    return "Google";
  }

  @Override
  public DataConverter converter() {
    return converter;
  }

}
