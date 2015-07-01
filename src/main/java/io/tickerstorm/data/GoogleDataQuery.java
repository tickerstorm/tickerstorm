package io.tickerstorm.data;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;

@Component
public class GoogleDataQuery extends BaseFileConverter implements QueryBuilder {

  private static final Logger logger = LoggerFactory.getLogger(GoogleDataQuery.class);

  public static final String HOST = "http://www.google.com/finance/getprices";
  public static final Integer MIN_1 = 60;

  private String symbol;
  private Integer period = 15;
  private String[] fields = new String[] { "d", "c", "h", "l", "o", "v" };
  private DataConverter converter;

  @Qualifier("historical")
  @Autowired
  private EventBus bus;

  public GoogleDataQuery(String symbol, DataConverter converter) {
    this.symbol = symbol;
    this.converter = converter;
  }

  public GoogleDataQuery(String symbol) {
    this.symbol = symbol;
    this.converter = this;
  }

  private GoogleDataQuery() {
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
    String symbol = this.symbol;
    DateTime timestamp = null;

    while (iterator.hasNext()) {

      String line = iterator.next();

      int offset = -240;
      if (line.contains("TIMEZONE_OFFSET")) {
        offset = Integer.valueOf(line.split("=")[1]);
      }

      if (line.contains("SYMBOL") && StringUtils.isEmpty(symbol)) {
        symbol = line.split("=")[1];
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
      c.symbol = symbol;
      c.close = new BigDecimal(args[1]);
      c.high = new BigDecimal(args[2]);
      c.low = new BigDecimal(args[3]);
      c.open = new BigDecimal(args[4]);
      c.volume = new BigDecimal(args[4]);
      c.timestamp = timestamp.plusMinutes(mins);
      c.interval = Candle.MIN_1_INTERVAL;
      c.source = "Google";
      md.add(c);

      if (bus != null)
        bus.post(c);

    }

    return md.toArray(new MarketData[] {});
  }

  @Override
  public Mode mode() {
    return Mode.doc;
  }

  @Override
  public void onFileCreate(File file) {

    if (file.getPath().contains(provider()) && Files.getFileExtension(file.getPath()).equals("csv")) {
      logger.info("Converting " + file.getPath());

      String symbol = Files.getNameWithoutExtension(file.getName());

      try (java.io.InputStream s = new FileInputStream(file.getPath())) {

        String content = new String("SYMBOL=" + symbol + "\n").concat(IOUtils.toString(s));
        convert(content);

      } catch (Exception e) {
        Throwables.propagate(e);
      }

      file.delete();
    }

  }

  @Override
  public void onDirectoryChange(File file) {

    if (file.getPath().contains(provider()) && !file.getName().equalsIgnoreCase(provider())) {
      if (file.isDirectory() && file.list().length == 0) {
        logger.info("Deleting " + file.getPath() + " since it's empty");
        file.delete();
      }
    }
  }

}
