/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.common.test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.tickerstorm.common.entity.Bar;
import java.io.File;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.commons.io.FileUtils;

public class TestDataFactory {
  
  static final SecureRandom r = new SecureRandom();

  public static List<Bar> buildCandles(int count, String symbol, String stream, BigDecimal open) {
    
    List<Bar> cs = Lists.newArrayList();
    Instant inst = Instant.now().minus(count + 1, ChronoUnit.MINUTES);

    for (int i = 0; i < count; i++) {

      BigDecimal close = randomRange(open, 0.03, 0.03);
      BigDecimal high = BigDecimal.valueOf(Math.max(close.doubleValue(), open.doubleValue())).multiply(BigDecimal.valueOf(1.03));
      BigDecimal low = BigDecimal.valueOf(Math.min(close.doubleValue(), open.doubleValue())).divide(BigDecimal.valueOf(1.03), 4,
          BigDecimal.ROUND_HALF_UP);
      BigDecimal vol = randomRange(new BigDecimal(23215D), new BigDecimal(43542345D));

      Bar c = new Bar(symbol, "Google", inst, open, close, high, low, Bar.MIN_1_INTERVAL, vol.intValue());
      c.stream = stream;
      cs.add(c);
      inst = inst.plus(1, ChronoUnit.MINUTES);
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
