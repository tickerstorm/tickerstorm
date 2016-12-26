package io.tickerstorm.common.test;

import java.io.File;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.beust.jcommander.internal.Lists;
import com.google.common.io.Files;

import io.tickerstorm.common.entity.Bar;

public class TestDataFactory {
  
  static final SecureRandom r = new SecureRandom();

  public static List<Bar> buildCandles(int count, String symbol, String stream, BigDecimal open) {
    
    List<Bar> cs = Lists.newArrayList();
    Instant inst = Instant.now();

    for (int i = 0; i < count; i++) {

      BigDecimal close = randomRange(open, 0.03, 0.03);
      BigDecimal high = BigDecimal.valueOf(Math.max(close.doubleValue(), open.doubleValue())).multiply(BigDecimal.valueOf(1.03));
      BigDecimal low = BigDecimal.valueOf(Math.min(close.doubleValue(), open.doubleValue())).divide(BigDecimal.valueOf(1.03), 4,
          BigDecimal.ROUND_HALF_UP);
      BigDecimal vol = randomRange(new BigDecimal(23215D), new BigDecimal(43542345D));

      Bar c = new Bar(symbol, "Google", inst, open, close, high, low, Bar.MIN_1_INTERVAL, vol.intValue());
      c.stream = stream;
      cs.add(c);
      inst = inst.plus(60, ChronoUnit.SECONDS);
    }

    return cs;

  }

  public static void storeGoogleData(String location) throws Exception {

    FileUtils.forceMkdir(new File(location + "/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(location + "/Google/TOL.csv"));
    Thread.sleep(5000);
    FileUtils.deleteQuietly(new File(location + "/Google/TOL.csv"));

  }

  public static BigDecimal randomRange(BigDecimal min, BigDecimal max) {
    
    BigDecimal range = max.subtract(min);
    BigDecimal result = min.add(range.multiply(new BigDecimal(r.nextDouble())));
    return result.abs();

  }

  public static BigDecimal randomRange(BigDecimal midPoint, Double lower, Double upper) {

    return randomRange(midPoint.multiply(BigDecimal.ZERO.subtract(BigDecimal.valueOf(Math.abs(lower)))),
        BigDecimal.ONE.add(BigDecimal.valueOf(upper))).abs();

  }
}
