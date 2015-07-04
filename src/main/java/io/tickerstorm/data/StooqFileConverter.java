package io.tickerstorm.data;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;

@Component
public class StooqFileConverter extends BaseFileConverter implements DataConverter {

  private static final java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final java.time.format.DateTimeFormatter dayFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(StooqFileConverter.class);

  private Set<String> securityTypes = new HashSet<String>();

  @Qualifier("historical")
  @Autowired
  private EventBus bus;

  @PostConstruct
  private void init() {
    securityTypes.add("currencies");
    securityTypes.add("indices");
  }

  @Override
  public MarketData[] convert(String path) {

    File f = new File(path);
    List<MarketData> data = new ArrayList<MarketData>();

    if (f.isFile()) {

      String symbol = f.getName().substring(0, f.getName().indexOf("."));

      for (String s : securityTypes) {

        if (f.getPath().contains(s)) {

          List<String> lines = new ArrayList<String>();

          try {
            lines = IOUtils.readLines(new FileInputStream(new File(path)));
          } catch (Exception e) {
            logger.error("Unable to read lines from file " + path);
            continue;
          }

          for (String line : lines) {

            if (line.startsWith("Date"))
              continue;

            Candle c = new Candle();
            try {

              String[] cols = line.split(",");

              c.symbol = symbol;

              try {

                c.timestamp = LocalDateTime.parse(cols[0] + " " + cols[1], formatter).toInstant(ZoneOffset.UTC);
                c.open = new BigDecimal(cols[2]);
                c.high = new BigDecimal(cols[3]);
                c.low = new BigDecimal(cols[4]);
                c.close = new BigDecimal(cols[5]);
                c.volume = new BigDecimal(cols[6]);

              } catch (Exception ex) {

                c.timestamp = LocalDateTime.from(dayFormatter.parse(cols[0])).toInstant(ZoneOffset.of("GMT"));
                c.open = new BigDecimal(cols[1]);
                c.high = new BigDecimal(cols[2]);
                c.low = new BigDecimal(cols[3]);
                c.close = new BigDecimal(cols[4]);
                c.volume = new BigDecimal(cols[5]);
              }

              if (BigInteger.ZERO.equals(c.volume))
                c.volume = null;

              c.interval = interval(path);
              c.source = provider();

            } catch (Exception e) {
              logger.error("Unable to parse symbol " + symbol, e.getMessage());
              continue;
            }

            bus.post(c);
            data.add(c);
          }
        }
      }
    }

    return data.toArray(new MarketData[] {});

  }

  private String interval(String path) {

    if (path.contains("5 min"))
      return Candle.MIN_5_INTERVAL;

    if (path.contains("hourly"))
      return Candle.HOURLY_INTERVAL;

    if (path.contains("daily"))
      return Candle.EOD;

    return Candle.MIN_5_INTERVAL;

  }

  @Override
  public String provider() {
    return "Stooq";
  }

  @Override
  public void onFileCreate(File file) {

    File f = new File(file.getPath().replace("\\", "\\\\"));

    if (file.getPath().contains(provider()) && Files.getFileExtension(file.getPath()).equals("txt")) {
      logger.info("Converting " + file.getName());
      convert(f.getPath());
      FileUtils.deleteQuietly(file);
    }
  }

  @Override
  public void onDirectoryChange(File file) {

    if (file.getPath().contains(provider()) && !file.getName().equalsIgnoreCase(provider())) {
      if (file.isDirectory() && file.list().length == 0) {
        logger.info("Deleting " + file.getPath() + " since it's empty");
        FileUtils.deleteQuietly(file);
      }
    }
  }
}
