package io.tickerstorm.data.query;

import io.tickerstorm.data.converter.DataConverter;
import io.tickerstorm.entity.Candle;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YahooChartsDataQuery implements QueryBuilder, DataConverter {

  public static final Logger logger = LoggerFactory.getLogger(YahooChartsDataQuery.class);

  public static final String HOST = "http://chartapi.finance.yahoo.com/instrument/1.0/";
  private String symbol;
  private String range = "9d";

  private TimeZone timezone;

  @Override
  public DataConverter converter() {
    return this;
  }

  public YahooChartsDataQuery(String symbol) {
    this.symbol = symbol;

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

  public Candle[] convert(String line) {

    if (line.contains("timezone:")) {
      this.timezone = TimeZone.getTimeZone(line.split(":")[1]);
      return null;
    }

    if (line.contains(":")) {
      return null;
    }

    String[] args = line.split(",");

    Candle c = new Candle();
    c.timestamp = Instant.ofEpochSecond(Long.valueOf(args[0]));
    c.low = new BigDecimal(args[3]);
    c.high = new BigDecimal(args[2]);
    c.close = new BigDecimal(args[1]);
    c.open = new BigDecimal(args[4]);
    c.volume = new Integer(args[5]);
    c.symbol = symbol;
    c.interval = Candle.MIN_5_INTERVAL;
    c.source = "yahoo";

    return new Candle[] {c};
  }

  public String type() {
    return Candle.TYPE;
  }

  public String symbol() {
    return symbol;
  }

  @Override
  public String provider() {
    return "Yahoo";
  }

  @Override
  public Mode mode() {
    return Mode.line;
  }

}
