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
import io.tickerstorm.common.reactive.CompletionTracker;
import io.tickerstorm.common.reactive.Observer;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class MarketDataCassandraSinkITCase {

  private final static Logger logger = LoggerFactory.getLogger(MarketDataCassandraSinkITCase.class);

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  private EventBus historicalBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Autowired
  private MarketDataDao dao;

  private String stream = "MarketDataCassandraSinkITCase".toLowerCase();

  @Before
  public void setup() throws Exception {
    logger.info("@Before");
    dao.deleteByStream(stream);
    Thread.sleep(3000);
  }

  @After
  public void cleanup() throws Exception {
    logger.info("@After");
    dao.deleteByStream(stream);
  }

  @Test
  public void storeCandle() throws Exception {

    final Bar c = new Bar();
    c.high = BigDecimal.TEN;
    c.low = BigDecimal.ONE;
    c.interval = Bar.MIN_10_INTERVAL;
    c.open = BigDecimal.ZERO;
    c.close = new BigDecimal("34.435");
    c.timestamp = Instant.now();
    c.volume = 10;
    c.symbol = "AAPL";
    c.stream = stream;
    historicalBus.post(c);

    logger.info("@Test");

    final AtomicBoolean done = new AtomicBoolean(false);

    Observer.observe(notificationsBus, "marketdata")
        .startCountDownOn(CompletionTracker.MarketData.isSaved(stream))
        .completeWhen(CompletionTracker.MarketData.isSaved(stream))
        .whenTimedOut(() -> {

          Assert.fail("No market data persisted");

        }).whenComplete((n) -> {

      logger.info("@Verifying");

      long count = dao.count(c.stream);

      Assert.assertEquals(count, 1);

      Stream<MarketDataDto> result = dao.findAll(c.stream);
      List<MarketDataDto> rs = result.collect(Collectors.toList());

      Assert.assertEquals(rs.size(), 1);

      for (MarketDataDto dto : rs) {

        Assert.assertEquals(dto.primarykey.stream, c.stream);
        Assert.assertEquals(dto.close, c.close);
        Assert.assertEquals(dto.low, c.low);
        Assert.assertEquals(dto.high, c.high);
        Assert.assertEquals(dto.open, c.open);
        Assert.assertEquals(dto.volume, new BigDecimal(c.volume));
        Assert.assertEquals(dto.primarykey.interval, c.interval);
        Assert.assertEquals(dto.primarykey.timestamp, Date.from(c.timestamp));
        Assert.assertEquals(dto.primarykey.symbol, c.symbol.toLowerCase());
        Assert.assertNotNull(dto.primarykey.date);

        Bar d = (Bar) dto.toMarketData(c.stream);

        Assert.assertEquals(d.stream, c.stream);
        Assert.assertEquals(d.close, c.close);
        Assert.assertEquals(d.low, c.low);
        Assert.assertEquals(d.high, c.high);
        Assert.assertEquals(d.open, c.open);
        Assert.assertEquals(d.volume, c.volume);
        Assert.assertEquals(d.interval, c.interval);
        Assert.assertTrue(d.timestamp.compareTo(c.timestamp) == 0);
        Assert.assertNotNull(d.timestamp);
        Assert.assertNotNull(c.timestamp);
        Assert.assertEquals(d.symbol, c.symbol.toLowerCase());

      }

      done.set(true);

    }).start();

    while (!done.get()) {
      Thread.sleep(2000);
    }

  }
}
