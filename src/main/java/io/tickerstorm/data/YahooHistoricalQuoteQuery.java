package io.tickerstorm.data;

import io.tickerstorm.entity.Candle;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YahooHistoricalQuoteQuery implements DataConverter, QueryBuilder {

  public static final Logger logger = LoggerFactory.getLogger(YahooHistoricalQuoteQuery.class);

  public static final String HOST = "http://ichart.yahoo.com/table.csv?";
  public static final String EOD = "d";
  public static final String WEEK = "w";
  public static final String MONTH = "m";

  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

  private String symbol;
  private LocalDateTime from = LocalDateTime.now().minusDays(20);
  private LocalDateTime until = LocalDateTime.now();
  private String interval = EOD;

  public YahooHistoricalQuoteQuery eod() {
    this.interval = EOD;
    return this;
  }

  @Override
  public Mode mode() {
    return Mode.line;
  }

  public YahooHistoricalQuoteQuery week() {
    this.interval = WEEK;
    return this;
  }

  public YahooHistoricalQuoteQuery month() {
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

  public YahooHistoricalQuoteQuery from(LocalDateTime from) {
    this.from = from;
    return this;
  }

  public YahooHistoricalQuoteQuery until(LocalDateTime until) {
    this.until = until;
    return this;
  }

  public String build() {
    String url = HOST + "&s=" + symbol + "&a=" + (from.get(ChronoField.YEAR) - 1) + "&b=" + from.get(ChronoField.MONTH_OF_YEAR) + "&c="
        + from.get(ChronoField.YEAR) + "&d=" + (until.get(ChronoField.YEAR) - 1) + "&e=" + until.get(ChronoField.MONTH_OF_YEAR) + "&f="
        + until.get(ChronoField.YEAR) + "&g=" + interval + "&ignore=.csv";

    logger.info(url);
    return url;
  }

  public Candle[] convert(String line) {

    if (line.contains("Date"))
      return null;

    String[] args = line.split(",");

    Candle c = new Candle();
    c.timestamp = LocalDate.parse(args[0], FORMATTER).atTime(0, 0).toInstant(ZoneOffset.ofHours(-7));
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

    c.source = "yahoo";

    return new Candle[] { c };
  }

  @Override
  public String provider() {
    return "Yahoo";
  }

}
