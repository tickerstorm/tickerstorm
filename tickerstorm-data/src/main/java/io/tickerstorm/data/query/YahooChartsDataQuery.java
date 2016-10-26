package io.tickerstorm.data.query;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tickerstorm.common.data.converter.DataQuery;
import io.tickerstorm.common.entity.Bar;

public class YahooChartsDataQuery implements QueryBuilder, DataQuery {

  public static final Logger logger = LoggerFactory.getLogger(YahooChartsDataQuery.class);

  public static final String HOST = "http://chartapi.finance.yahoo.com/instrument/1.0/";
  private String symbol;
  private String range = "9d";

  public YahooChartsDataQuery(String symbol) {
    this.symbol = symbol;
  }

  @Override
  public String namespace() {
    return HOST;
  }

  public YahooChartsDataQuery withSymbol(String symbol) {
    this.symbol = symbol;
    return this;
  }

  public String build() {
    String url = HOST + symbol + "/chartdata;type=quote;range=" + range + "/csv/";
    logger.info(url);
    return url;
  }

  public String type() {
    return Bar.TYPE;
  }

  @Override
  public String getInterval() {
    return Bar.MIN_5_INTERVAL;
  }

  public String getSymbol() {
    return symbol;
  }

  @Override
  public String provider() {
    return "Yahoo";
  }

  @Override
  public Map<String, String> headers() {
    // TODO Auto-generated method stub
    return null;
  }


}
