package io.tickerstorm.data.query;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tickerstorm.common.data.query.DataQuery;

public class YahooHistoricalQuoteQuery implements QueryBuilder, DataQuery {

  public String getSymbol() {
    return symbol;
  }

  public String getInterval() {
    return interval;
  }

  @Override
  public String namespace() {
    return HOST;
  }


  public static final Logger logger = LoggerFactory.getLogger(YahooHistoricalQuoteQuery.class);

  public static final String HOST = "http://ichart.yahoo.com/table.csv?";
  public static final String EOD = "d";
  public static final String WEEK = "w";
  public static final String MONTH = "m";

  private String symbol;
  private LocalDateTime from = LocalDateTime.now().minusDays(20);
  private LocalDateTime until = LocalDateTime.now();
  private String interval = EOD;

  public YahooHistoricalQuoteQuery eod() {
    this.interval = EOD;
    return this;
  }

  public YahooHistoricalQuoteQuery week() {
    this.interval = WEEK;
    return this;
  }

  public YahooHistoricalQuoteQuery month() {
    this.interval = MONTH;
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
