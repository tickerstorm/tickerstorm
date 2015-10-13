package io.tickerstorm.data.converter;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.tickerstorm.data.query.DataQuery;
import io.tickerstorm.data.query.GoogleDataQuery;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

@Component
public class GoogleFileConverter extends BaseFileConverter implements FileConverter, DataConverter {

  private final static Logger logger = LoggerFactory.getLogger(GoogleFileConverter.class);

  @Override
  public Set<String> namespaces() {
    return Sets.newHashSet(GoogleDataQuery.HOST);
  }

  @Override
  public void onFileCreate(File file) {

    if (file.getPath().contains(provider()) && Files.getFileExtension(file.getPath()).equals("csv")) {

      String symbol = Files.getNameWithoutExtension(file.getName());

      try (java.io.InputStream s = new FileInputStream(file.getPath())) {

        String content = new String("SYMBOL=" + symbol + "\n").concat(IOUtils.toString(s));
        logger.info("Converting " + file.getPath());
        long start = System.currentTimeMillis();
        MarketData[] data = convert(content);
        logger.info("Converting " + data.length + " records took " + (System.currentTimeMillis() - start) + "ms");

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

  @Override
  public MarketData[] convert(String line) {
    return convert(line, new DummyQuery());
  }

  private class DummyQuery implements DataQuery {

    @Override
    public String namespace() {
      return "NotApplicable";
    }

    @Override
    public String getSymbol() {
      return null;
    }

    @Override
    public String getInterval() {
      return null;
    }

    @Override
    public String build() {
      return null;
    }

    @Override
    public String provider() {
      return "Google";
    }

  }

  public MarketData[] convert(String doc, DataQuery dq) {

    List<MarketData> md = new ArrayList<MarketData>();
    LineIterator iterator = IOUtils.lineIterator(new StringReader(doc));
    Instant timestamp = null;
    String symbol = dq.getSymbol();

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
        timestamp = Instant.ofEpochSecond(Long.valueOf(t));
      } else {
        mins = Integer.valueOf(args[0]);
      }

      Candle c = new Candle(symbol, "Google", timestamp.plus(mins, ChronoUnit.MINUTES), new BigDecimal(args[4]), new BigDecimal(args[1]),
          new BigDecimal(args[2]), new BigDecimal(args[3]), Candle.MIN_1_INTERVAL,
          new BigDecimal(args[5]).multiply(new BigDecimal("1000")).intValue());

      if (historical != null)// in case being invoked standalone (i.e. tests)
        historical.publishAsync(c);

      md.add(c);
    }

    return md.toArray(new MarketData[] {});
  }

  @Override
  public String provider() {
    return "Google";
  }


  @Override
  public Mode mode() {
    return Mode.doc;
  }

}
