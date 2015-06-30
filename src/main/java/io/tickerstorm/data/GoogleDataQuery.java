package io.tickerstorm.data;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class GoogleDataQuery implements QueryBuilder, DataConverter {

  public static final String HOST = "http://www.google.com/finance/getprices";
  public static final Integer MIN_1 = 60;

  private String symbol;
  private Integer period = 15;
  private String[] fields = new String[] { "d", "c", "h", "l", "o", "v" };
  private DataConverter converter;

  public GoogleDataQuery(String symbol, DataConverter converter) {
    this.symbol = symbol;
    this.converter = converter;
  }

  public GoogleDataQuery(String symbol) {
    this.symbol = symbol;
    this.converter = this;
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

  @Override
  public DataConverter converter() {
    return converter;
  }

  public GoogleDataQuery days(int i) {

    if (i < 16 && i > 0)
      period = i;

    return this;
  }

  @Override
  public MarketData[] convert(String doc) {

    List<MarketData> md = new ArrayList<MarketData>();
    LineIterator iterator = IOUtils.lineIterator(new StringReader(doc));

    DateTime timestamp = null;

    while (iterator.hasNext()) {

      String line = iterator.next();

      int offset = -240;
      if (line.contains("TIMEZONE_OFFSET")) {
        offset = Integer.valueOf(line.split("=")[1]);
      }

      if (line.contains("=") || line.contains("EXCHANGE"))
        continue;

      int mins = 0;
      String[] args = line.split(",");

      if (args[0].startsWith("a")) {
        String t = args[0].replace("a", "");
        timestamp = new DateTime(new Date(Long.valueOf(t) * 1000)).withZone(DateTimeZone.forOffsetHours(offset / 60));
      } else {
        mins = Integer.valueOf(args[0]);
      }

      Candle c = new Candle();
      c.symbol = this.symbol;
      c.close = new BigDecimal(args[1]);
      c.high = new BigDecimal(args[2]);
      c.low = new BigDecimal(args[3]);
      c.open = new BigDecimal(args[4]);
      c.volume = new BigDecimal(args[4]);
      c.timestamp = timestamp.plusMinutes(mins);
      c.interval = Candle.MIN_1_INTERVAL;
      c.source = "Google";
      md.add(c);

    }

    return md.toArray(new MarketData[] {});
  }

  @Override
  public Mode mode() {
    return Mode.doc;
  }

}
