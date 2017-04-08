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

package io.tickerstorm.data.dao;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
public class CassandraPerformanceITCase {

  private final static org.slf4j.Logger logger = LoggerFactory
      .getLogger(CassandraPerformanceITCase.class);

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Autowired
  private ModelDataDao dao;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  private final List<ModelDataDto> dtos = new ArrayList<>();
  private final List<Bar> cs = new ArrayList<>();
  private final String stream = "CassandraPerformanceITCase";

  @Before
  public void clean() throws Exception {
    dao.deleteByStream(stream);
    Thread.sleep(2000);

    long time = System.currentTimeMillis();
    for (int j = 0; j < 2000; j++) {
      Bar c =
          new Bar("Goog", stream, Instant.now().plus(1, ChronoUnit.MINUTES),
              new BigDecimal(Math.random()), new BigDecimal(Math.random()),
              new BigDecimal(Math.random()), new BigDecimal(Math.random()), "1m",
              Double.valueOf(Math.random()).intValue());

      for (int i = 0; i < 3; i++) {
        for (Field<?> f : c.getFields()) {
          c.getFields().add(new BaseField<BigDecimal>(f, "test-field-p" + i, new BigDecimal(Math.random())));
        }
      }

      cs.add(c);
      ModelDataDto dto = ModelDataDto.convert(c);
      dtos.add(dto);

    }
    logger.info(
        "Converting market data of size " + dtos.size() + " took " + (System.currentTimeMillis()
            - time) + "ms");
  }

  @Test
  public void testPureCassandraStorage() {

    long time = System.currentTimeMillis();
    dao.ingest(dtos);
    logger.info(
        "Pure cassandra storage of " + dtos.size() + " took " + (System.currentTimeMillis() - time)
            + "ms");

  }

  @Test
  public void testStoreToCassandraViaModelDataBus() throws Exception {

    final AtomicInteger i = new AtomicInteger(0);
    long time = System.currentTimeMillis();
    notificationsBus.register(new CassandraStorageEventListener(time));
    cs.forEach(c -> {
      c.getFields().forEach(f -> {
        i.addAndGet(1);
        modelDataBus.post(f);
      });
    });

    logger.info("Dispatching " + i + " fields took " + (System.currentTimeMillis() - time) + "ms");
    Thread.sleep(8000);

  }

  private class CassandraStorageEventListener {

    long time = 0L;
    final AtomicInteger count = new AtomicInteger(0);

    public CassandraStorageEventListener(long start) {
      this.time = start;
    }

    @Subscribe
    public void onNotification(Serializable s) {

      if (s instanceof Notification) {

        Notification n = (Notification) s;

        synchronized (count) {
          if (n.markers.contains(Markers.MODEL_DATA.toString()) && n.markers.contains(Markers.SAVE.toString())) {
            count.addAndGet(n.expect);
          }
        }

        System.out.println("Marker " + count.get());

        if (count.get() == 18000) {
          long took = (n.eventTime.toEpochMilli() - time);
          logger.info("Persisting " + count.get() + " fields took " + took + "ms");
          if (took > 2000) {
            Assert.fail("Persisting model events took longer than 2s");
          }
        }
      }

    }

  }

}
