package io.tickerstorm.data.converter;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

@Component
public class DukascopyFileConverter extends BaseFileConverter {

  private static final java.time.format.DateTimeFormatter formatter =
      java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss.SSS");

  private static final Logger logger = LoggerFactory.getLogger(DukascopyFileConverter.class);

  @Override
  public MarketData[] convert(String path) {

    File f = new File(path);
    List<MarketData> data = new ArrayList<MarketData>();

    if (f.isDirectory()) {

      for (File s : f.listFiles()) {
        data.addAll(Lists.newArrayList(convert(s.getPath())));
      }

    } else if (f.isFile() && Files.getFileExtension(path).equalsIgnoreCase("csv")) {

      String currency = path.substring(0, path.indexOf("_"));

      try {

        List<String> lines = IOUtils.readLines(new FileInputStream(f));

        for (String l : lines) {

          if (l.contains("Time"))
            continue;

          String[] cols = l.split(",");
          Candle c = new Candle(currency, provider(), LocalDateTime.parse(cols[0], formatter).toInstant(ZoneOffset.UTC),
              new BigDecimal(cols[1]), new BigDecimal(cols[4]), new BigDecimal(cols[2]), new BigDecimal(cols[3]), getInterval(path),
              new BigDecimal(cols[5]).multiply(new BigDecimal("1000000")).intValue());

          historical.publishAsync(c);
          data.add(c);

        }

      } catch (Exception e) {
        Throwables.propagate(e);
      }

    }

    return data.toArray(new MarketData[] {});
  }

  @Override
  public String provider() {
    return "Dukascopy";
  }

  private String getInterval(String fileName) {

    if (fileName.contains("_1_m_")) {
      return Candle.MIN_1_INTERVAL;
    } else if (fileName.contains("_10_m_")) {
      return Candle.MIN_10_INTERVAL;
    } else if (fileName.contains("_5_m_")) {
      return Candle.MIN_5_INTERVAL;
    } else if (fileName.contains("_1_h_")) {
      return Candle.HOURLY_INTERVAL;
    } else if (fileName.contains("_1_d_")) {
      return Candle.EOD;
    }

    return Candle.EOD;
  }

  @Override
  public void onFileCreate(File file) {

    file = new File(file.getPath().replace("\\", "\\\\"));

    if (file.getPath().contains(provider()) && Files.getFileExtension(file.getPath()).equals("csv")) {
      logger.info("Converting " + file.getPath());
      long start = System.currentTimeMillis();
      MarketData[] data = convert(file.getPath());
      logger.info("Converted " + data.length + " records.");
      logger.debug("Conversion took " + (System.currentTimeMillis() - start) + "ms");
      file.delete();
    }

  }

  @Override
  public void onDirectoryChange(File file) {

    file = new File(file.getPath().replace("\\", "\\\\"));

    if (file.getPath().contains(provider()) && !file.getName().equalsIgnoreCase(provider())) {
      if (file.isDirectory() && file.list().length == 0) {
        logger.info("Deleting " + file.getPath() + " since it's empty");
        file.delete();
      }
    }
  }

  @Override
  public Mode mode() {
    return Mode.file;
  }
}
