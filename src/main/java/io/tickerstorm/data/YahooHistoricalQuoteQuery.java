package io.tickerstorm.data;

import io.tickerstorm.entity.Candle;

import java.math.BigDecimal;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YahooHistoricalQuoteQuery implements DataConverter, QueryBuilder {
  
  public static final Logger logger = LoggerFactory.getLogger(YahooHistoricalQuoteQuery.class);

  public static final String HOST = "http://ichart.yahoo.com/table.csv?";
  public static final String EOD = "d";
  public static final String WEEK = "w";
  public static final String MONTH = "m";

  public static final org.joda.time.format.DateTimeFormatter FORMATTER = ISODateTimeFormat.date();

  private String symbol;
  private DateTime from = new DateTime().minusDays(20).withZone(DateTimeZone.forID("EST"));
  private DateTime until = new DateTime().withZone(DateTimeZone.forID("EST"));
  private String interval = EOD;
  
  public YahooHistoricalQuoteQuery eod(){
    this.interval = EOD;
    return this;
  }
  
  @Override
  public Mode mode() {
    return Mode.line;
  }
  
  public YahooHistoricalQuoteQuery week(){
    this.interval = WEEK;
    return this;
  }
  
  public YahooHistoricalQuoteQuery month(){
    this.interval = MONTH;
    return this;
  }
  
  @Override
  public DataConverter converter() {
    return this;
  }

  public YahooHistoricalQuoteQuery(String symbol) {
    this.symbol = symbol;
  }

  public YahooHistoricalQuoteQuery withSymbol(String symbol) {
    this.symbol = symbol;
    return this;
  }

  public YahooHistoricalQuoteQuery from(DateTime from) {
    this.from = from;
    return this;
  }

  public YahooHistoricalQuoteQuery until(DateTime until) {
    this.until = until;
    return this;
  }

  public String build() {
    String url = HOST + "&s=" + symbol + "&a=" + (from.getMonthOfYear() - 1) + "&b=" + from.getDayOfMonth() + "&c=" + from.getYear() + "&d="
        + (until.getMonthOfYear() - 1) + "&e=" + until.getDayOfMonth() + "&f=" + until.getYear() + "&g=" + interval + "&ignore=.csv";
    
    logger.info(url);
    return url;
  }

  public Candle[] convert(String line) {

    if (line.contains("Date"))
      return null;

    String[] args = line.split(",");

    Candle c = new Candle();
    c.timestamp = FORMATTER.parseDateTime(args[0]).withZone(DateTimeZone.forID("EST"));
    c.open = new BigDecimal(args[1]);
    c.high = new BigDecimal(args[2]);
    c.low = new BigDecimal(args[3]);
    c.close = new BigDecimal(args[4]);
    c.volume = new BigDecimal(args[5]);
    c.symbol = symbol;

    if (interval.equals(EOD))
      c.interval = Candle.EOD;
    if (interval.equals(WEEK))
      c.interval = Candle.WEEK_INTERVAL;
    if (interval.equals(MONTH))
      c.interval = Candle.MONTH_INTERVAL;

    c.source = "yahoo";

    return new Candle[]{c};
  }
  
  @Override
  public String provider() {
    return "Yahoo";
  }

}
