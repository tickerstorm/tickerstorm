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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field.Name;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Quote;
import io.tickerstorm.common.entity.Tick;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.influxdb.dto.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class TestInfluxMarketDataDao {

  @Autowired
  private InfluxMarketDataDao dao;

  @Before
  public void cleanup() {
    dao.newDelete().bySource("TestMarketDataDto").delete();
    dao.newDelete().bySource("TestMarketDataDto2").delete();
  }

  @Test
  public void convertCandle() {

    Bar c = new Bar();
    c.close = BigDecimal.TEN;
    c.open = BigDecimal.TEN;
    c.low = BigDecimal.TEN;
    c.high = BigDecimal.TEN;
    c.stream = "test";
    c.symbol = "AAPL";
    c.interval = Bar.MIN_1_INTERVAL;
    c.timestamp = Instant.now();
    c.volume = 0;

    InfluxMarketDataDto dto = InfluxMarketDataDto.convert(c);
    Point p = dto.getPoint();

    assertEquals(p.getFields().get(Name.CLOSE.field()), c.close.doubleValue());
    assertEquals(p.getFields().get(Name.LOW.field()), c.low.doubleValue());
    assertEquals(p.getFields().get(Name.HIGH.field()), c.high.doubleValue());
    assertEquals(p.getFields().get(Name.OPEN.field()), c.open.doubleValue());
    assertEquals(p.getFields().get(Name.VOLUME.field()), BigDecimal.valueOf(c.volume).intValue());
    assertEquals(p.getTags().get(Name.SOURCE.field()), c.stream);
    assertEquals(p.getTags().get(Name.INTERVAL.field()), c.interval.toLowerCase());
    assertEquals(p.getTime(), Long.valueOf(c.timestamp.toEpochMilli()));
    assertEquals(p.getMeasurement(), c.symbol.toLowerCase());

    Bar d = (Bar) dto.toMarketData();

    assertEquals(d.close, c.close);
    assertEquals(d.low, c.low);
    assertEquals(d.high, c.high);
    assertEquals(d.open, c.open);
    assertEquals(d.volume, c.volume);
    assertEquals(d.stream, c.stream.toLowerCase());
    assertEquals(d.interval, c.interval.toLowerCase());

    // assertEquals(d.timestamp, c.timestamp); not working
    assertNotNull(d.timestamp);
    assertNotNull(c.timestamp);
    assertEquals(d.symbol, c.symbol);

  }

  @Test
  public void testSelectBars() throws Exception {

    List<Bar> bars = TestDataFactory.buildCandles(5, "goog", "TestMarketDataDto", new BigDecimal("34.4354"));
    dao.ingest((Collection) bars);

    bars = TestDataFactory.buildCandles(2, "tol", "TestMarketDataDto2", new BigDecimal("12.243"));
    dao.ingest((Collection) bars);

    Thread.sleep(1000);

    long count = dao.newCount(Bar.TYPE).bySource("TestMarketDataDto").count();
    org.junit.Assert.assertEquals(5, count);

    count = dao.newCount(Bar.TYPE).bySource("TestMarketDataDto2").count();
    org.junit.Assert.assertEquals(2, count);

    List<MarketData> dtoss = dao.findAll("TestMarketDataDto");
    org.junit.Assert.assertEquals(5, dtoss.size());

    dtoss = dao.findAll("TestMarketDataDto2");
    org.junit.Assert.assertEquals(2, dtoss.size());

    dao.newDelete().bySource("TestMarketDataDto").delete();

    count = dao.newCount(Bar.TYPE).bySource("TestMarketDataDto").count();
    org.junit.Assert.assertEquals(0, count);

    count = dao.newCount(Bar.TYPE).bySource("TestMarketDataDto2").count();
    org.junit.Assert.assertEquals(2, count);

    dao.newDelete().bySource("TestMarketDataDto2").delete();

    count = dao.newCount(Bar.TYPE).bySource("TestMarketDataDto2").count();
    org.junit.Assert.assertEquals(0, count);
  }

  /**
   * Not currently supported
   */
  // @Test
  public void convertQuote() {

    Quote c = new Quote("AAPL", "test", Instant.now());
    c.ask = BigDecimal.TEN;
    c.bid = BigDecimal.TEN;
    c.askSize = 0;
    c.bidSize = 0;

    InfluxMarketDataDto dto = InfluxMarketDataDto.convert(c);
    Point p = dto.getPoint();

    assertEquals(p.getFields().get(Name.ASK.field()), c.ask);
    assertEquals(p.getFields().get(Name.BID.field()), c.bid);
    assertEquals(p.getFields().get(Name.ASK_SIZE.field()), c.askSize);
    assertEquals(p.getFields().get(Name.BID_SIZE.field()), c.bidSize);
    assertEquals(p.getTags().get(Name.SOURCE.field()), c.stream);
    assertEquals(p.getTime(), Date.from(c.timestamp));
    assertEquals(p.getMeasurement(), c.symbol.toLowerCase());

    Quote d = (Quote) dto.toMarketData();

    assertEquals(d.ask, c.ask);
    assertEquals(d.bid, c.bid);
    assertEquals(d.askSize, c.askSize);
    assertEquals(d.bidSize, c.bidSize);
    assertEquals(d.stream, c.stream.toLowerCase());

    // assertEquals(d.timestamp, c.timestamp); not working
    assertNotNull(d.timestamp);
    assertNotNull(c.timestamp);
    assertEquals(d.symbol, c.symbol.toLowerCase());

  }

  /**
   * Not currently supported
   */
  // @Test
  public void convertTick() {

    Tick c = new Tick("AAPL", "test", Instant.now());
    c.price = BigDecimal.TEN;
    c.quantity = BigDecimal.TEN;
    InfluxMarketDataDto dto = InfluxMarketDataDto.convert(c);
    Point p = dto.getPoint();

    assertEquals(p.getFields().get(Name.PRICE.field()), c.price);
    assertEquals(p.getFields().get(Name.QUANTITY.field()), c.quantity);
    assertEquals(p.getTags().get(Name.SOURCE.field()), c.stream);
    assertEquals(p.getTime(), Date.from(c.timestamp));
    assertEquals(p.getMeasurement(), c.symbol.toLowerCase());

    Tick d = (Tick) dto.toMarketData();

    assertEquals(d.price, c.price);
    assertEquals(d.quantity, c.quantity);
    assertEquals(d.stream, c.stream.toLowerCase());

    // assertEquals(d.timestamp, c.timestamp); not working
    assertNotNull(d.timestamp);
    assertNotNull(c.timestamp);
    assertEquals(d.symbol, c.symbol.toLowerCase());

  }

}
