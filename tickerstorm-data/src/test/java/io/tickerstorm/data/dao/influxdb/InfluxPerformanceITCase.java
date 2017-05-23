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

package io.tickerstorm.data.dao.influxdb;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class InfluxPerformanceITCase {

  private final static org.slf4j.Logger logger = LoggerFactory.getLogger(InfluxPerformanceITCase.class);
  private final List<Bar> cs = new ArrayList<>();
  private final String stream = "InfluxPerformanceITCase";
  private final AtomicBoolean success = new AtomicBoolean(false);
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;
  @Autowired
  private InfluxModelDataDao dao;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  @Before
  public void init() throws Exception {

    long time = System.currentTimeMillis();
    Instant n = Instant.now();
    for (int j = 0; j < 2000; j++) {
      n = n.plus(1, ChronoUnit.MINUTES);
      final Bar c =
          new Bar("Goog", stream, n,
              new BigDecimal(Math.random()), new BigDecimal(Math.random()),
              new BigDecimal(Math.random()), new BigDecimal(Math.random()), "1m",
              new BigDecimal(Math.random()));

      for (int i = 0; i < 3; i++) {
        for (Field<?> f : c.getFields()) {
         // c.addField(new BaseField<BigDecimal>(f, "test-field-p" + i, new BigDecimal(Math.random())));
        }
      }
      cs.add(c);

    }
  }

  @After
  public void clean() throws Exception {
    dao.deleteByStream(stream);
  }

  @Test
  public void testPureMarketDataInfluxStorage() throws Exception {

    long time = System.currentTimeMillis();
    dao.ingestMarketData((Collection) cs);
    Thread.sleep(500);
    Assert.assertEquals(2000, dao.count(stream));
    logger.info(
        "Pure influx storage of " + cs.size() + " took " + (System.currentTimeMillis() - time)
            + "ms");
  }

  @Test
  public void testPureFieldInfluxStorage() throws Exception {

    long time = System.currentTimeMillis();
    final List<Field<?>> fs = new ArrayList<>();
    cs.stream().map(b -> {
      return b.getFields();
    }).forEach(s -> {
      fs.addAll(s);
    });

    dao.ingest(fs);
    Thread.sleep(500);
    Assert.assertEquals(2000, dao.count(stream));
    logger.info(
        "Pure influx storage of " + cs.size() + " took " + (System.currentTimeMillis() - time)
            + "ms");
  }

  @Test
  public void testStoreToInfluxViaModelDataBus() throws Exception {

    final AtomicInteger i = new AtomicInteger(0);
    final List<Field<?>> fs = new ArrayList<>();
    cs.stream().map(b -> {
      return b.getFields();
    }).forEach(s -> {
      fs.addAll(s);
    });

    long time = System.currentTimeMillis();
    notificationsBus.register(new InfluxStorageEventListener(time));
    fs.stream().forEach(f -> {
      modelDataBus.post(f);
    });

    logger.info("Dispatching " + fs.size() + " fields took " + (System.currentTimeMillis() - time) + "ms");
    Thread.sleep(1000);

    Assert.assertTrue(success.get());
  }

  private class InfluxStorageEventListener {

    final AtomicInteger count = new AtomicInteger(0);
    long time = 0L;

    public InfluxStorageEventListener(long start) {
      this.time = start;
    }

    @Subscribe
    public void onNotification(Serializable s) {

      if (s instanceof Notification) {

        Notification n = (Notification) s;

        if (n.markers.contains(Markers.MODEL_DATA.toString()) && n.markers.contains(Markers.SAVE.toString())) {
          count.addAndGet(n.expect);
        }

        logger.info("Confirmed " + n.expect + " out of " + count.get() + " total");

        if (count.get() == 10000) {//While we dispatch 18000 fields, timestamp, stream, symbol and interval become tags so they don't count in points count
          long took = (n.eventTime.toEpochMilli() - time);
          logger.info("Persisting " + count.get() + " fields took " + took + "ms");
          if (took > 700) {
            Assert.fail("Persisting model events took longer than 700ms");
          } else {
            success.set(true);
          }
        }
      }

    }

  }

}
