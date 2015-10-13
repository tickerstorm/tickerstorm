package io.tickerstorm.data.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tickerstorm.entity.Quote;

public class YahooRealtimeQuoteQuery implements QueryBuilder, DataQuery {

  public static final Logger logger = LoggerFactory.getLogger(YahooRealtimeQuoteQuery.class);

  public static final String HOST = "http://download.finance.yahoo.com/d/quotes.csv?s=";

  public String symbol;

  public YahooRealtimeQuoteQuery(String symbol) {
    this.symbol = symbol;
  }

  @Override
  public String namespace() {
    return HOST;
  }

  public YahooRealtimeQuoteQuery withSymbol(String symbol) {
    this.symbol = symbol;
    return this;
  }

  public String build() {

    String url = HOST + symbol + "&f=a0aa5bb2b6x";

    logger.info(url);
    return url;
  }

  @Override
  public String provider() {
    return "Yahoo";
  }

  @Override
  public String getSymbol() {
    return symbol;
  }

  @Override
  public String getInterval() {
    return Quote.TYPE;
  }

}
