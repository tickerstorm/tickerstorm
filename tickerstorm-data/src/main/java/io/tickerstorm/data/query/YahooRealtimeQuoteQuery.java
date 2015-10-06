package io.tickerstorm.data.query;

import io.tickerstorm.data.converter.DataConverter;
import io.tickerstorm.entity.Quote;

import java.math.BigDecimal;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YahooRealtimeQuoteQuery implements QueryBuilder, DataConverter {

  public static final Logger logger = LoggerFactory.getLogger(YahooRealtimeQuoteQuery.class);

  public static final String HOST = "http://download.finance.yahoo.com/d/quotes.csv?s=";

  public String symbol;

  public YahooRealtimeQuoteQuery(String symbol) {
    this.symbol = symbol;
  }

  public YahooRealtimeQuoteQuery withSymbol(String symbol) {
    this.symbol = symbol;
    return this;
  }

  @Override
  public DataConverter converter() {
    return this;
  }

  @Override
  public Mode mode() {
    return Mode.line;
  }

  public Quote[] convert(String line) {
    String[] data = line.split(",");

    Quote c = new Quote();
    c.symbol = symbol;
    c.source = "yahoo";
    c.timestamp = Instant.now();

    if (!data[0].equalsIgnoreCase("N/A"))
      c.ask = new BigDecimal(data[0]);
    else if (!data[1].equalsIgnoreCase("N/A"))
      c.ask = new BigDecimal(data[1]);
    else
      return null;

    c.askSize = new Integer(data[2]);

    if (!data[3].equalsIgnoreCase("N/A"))
      c.bid = new BigDecimal(data[3]);
    else if (!data[4].equalsIgnoreCase("N/A"))
      c.bid = new BigDecimal(data[4]);
    else
      return null;

    c.bidSize = new Integer(data[5]);

    return new Quote[] {c};
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

}
