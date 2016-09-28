package io.tickerstorm.data.converter;

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

import io.tickerstorm.common.data.converter.BaseFileConverter;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Notification;

@Component
public class StooqFileConverter extends BaseFileConverter {

  private static final java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final java.time.format.DateTimeFormatter dayFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");

  private static final Logger logger = LoggerFactory.getLogger(StooqFileConverter.class);

  private Set<String> securityTypes = new HashSet<String>();

  @PostConstruct
  private void init() {
    securityTypes.add("currencies");
    securityTypes.add("indices");
  }

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notifications;

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

            Candle c = null;
            try {

              String[] cols = line.split(",");

              try {

                c = new Candle(symbol, provider(), LocalDateTime.parse(cols[0] + " " + cols[1], formatter).toInstant(ZoneOffset.UTC),
                    new BigDecimal(cols[2]), new BigDecimal(cols[5]), new BigDecimal(cols[3]), new BigDecimal(cols[4]), interval(path),
                    new Integer(cols[6]));
                c.stream = provider();

              } catch (Exception ex) {

                c = new Candle(symbol, provider(), LocalDateTime.from(dayFormatter.parse(cols[0])).toInstant(ZoneOffset.of("GMT")),
                    new BigDecimal(cols[1]), new BigDecimal(cols[4]), new BigDecimal(cols[2]), new BigDecimal(cols[3]), interval(path),
                    new Integer(cols[5]));
                c.stream = provider();

              }

              if (BigInteger.ZERO.equals(c.volume)) {
                c.setVolume(null);
              }

              // c.interval = interval(path);
              // c.source = provider();

            } catch (Exception e) {
              logger.error("Unable to parse symbol " + symbol, e.getMessage());
              continue;
            }

            historical.post(c);
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

      try {

        Notification not = new Notification(provider());
        not.addMarker(Markers.FILE.toString());
        not.addMarker(Markers.INGEST.toString());
        not.addMarker(Markers.START.toString());
        not.addMarker(Markers.FILE_LOCATION.toString());
        not.properties.put(Markers.FILE_LOCATION.toString(), file.getPath());
        notifications.post(not);

        logger.info("Converting " + file.getName());
        long start = System.currentTimeMillis();
        MarketData[] data = convert(f.getPath());
        logger.info("Converted " + data.length + " records.");
        logger.debug("Conversion took " + (System.currentTimeMillis() - start) + "ms");
        FileUtils.deleteQuietly(file);

        not = new Notification(provider());
        not.addMarker(Markers.FILE.toString());
        not.addMarker(Markers.INGEST.toString());
        not.addMarker(Markers.SUCCESS.toString());
        not.addMarker(Markers.FILE_LOCATION.toString());
        not.properties.put(Markers.FILE_LOCATION.toString(), file.getPath());
        not.expect = data.length;
        notifications.post(not);

      } catch (Exception e) {

        Notification not = new Notification(provider());
        not.addMarker(Markers.FILE.toString());
        not.addMarker(Markers.INGEST.toString());
        not.addMarker(Markers.FAILED.toString());
        not.addMarker(Markers.FILE_LOCATION.toString());
        not.properties.put(Markers.FILE_LOCATION.toString(), file.getPath());
        notifications.post(not);

        logger.error(e.getMessage(), e);
      }
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
