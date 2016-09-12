package io.tickerstorm.common.test;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.beust.jcommander.internal.Lists;
import com.google.common.io.Files;

import io.tickerstorm.common.entity.Candle;

public class TestDataFactory {

  private static final Random r = new Random();

  public static List<Candle> buildCandles(int count, String symbol, String stream, BigDecimal open) {

    List<Candle> cs = Lists.newArrayList();

    for (int i = 0; i < count; i++) {

      BigDecimal close = randomRange(open, 0.03, 0.03);
      BigDecimal high = BigDecimal.valueOf(Math.max(close.doubleValue(), open.doubleValue())).multiply(BigDecimal.valueOf(1.03));
      BigDecimal low = BigDecimal.valueOf(Math.min(close.doubleValue(), open.doubleValue())).divide(BigDecimal.valueOf(1.03), 4, BigDecimal.ROUND_HALF_UP);
      BigDecimal vol = randomRange(new BigDecimal(2321513D), new BigDecimal(4354234562D));

      Candle c = new Candle(symbol, "Google", Instant.now(), open, close, high, low, Candle.MIN_1_INTERVAL, vol.intValue());
      c.stream = stream;
      cs.add(c);
    }

    return cs;

  }
  
  public static void storeGoogleData() throws Exception {
    
    FileUtils.forceMkdir(new File("./data/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File("./data/Google/TOL.csv"));
    Thread.sleep(5000);
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
    
  }

  public static BigDecimal randomRange(BigDecimal min, BigDecimal max) {

    BigDecimal range = max.subtract(min);
    BigDecimal result = min.add(range.multiply(new BigDecimal(r.nextDouble())));
    return result;

  }

  public static BigDecimal randomRange(BigDecimal midPoint, Double lower, Double upper) {

    return randomRange(midPoint.multiply(BigDecimal.ZERO.subtract(BigDecimal.valueOf(Math.abs(lower)))),
        BigDecimal.ONE.add(BigDecimal.valueOf(upper)));

  }
}
