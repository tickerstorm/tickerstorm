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

import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class InfluxModelDataSinkITCase {

  private final Instant instant = Instant.now();
  private final String symbol = "goog";
  private final String stream = "InfluxModelDataSinkITCase".toLowerCase();
  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;
  @Autowired
  private InfluxModelDataDao dao;
  private Bar c;

  @After
  public void cleanup() throws Exception {
    dao.deleteByStream(stream);
  }

  @Before
  public void init() throws Exception {
    c = new Bar(symbol, stream, this.instant, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", new BigDecimal(1000));
  }

  @Test
  public void test1StoreModelData() throws Exception {

    BaseField<BigDecimal> df = new BaseField<BigDecimal>(c.getEventId(), "ave", new BigDecimal(100));
    BaseField<BigDecimal> cf = new BaseField<BigDecimal>(c.getEventId(), "sma", BigDecimal.TEN);

    modelDataBus.post(c);
    modelDataBus.post(df);
    modelDataBus.post(cf);

    Thread.sleep(1000);

    long count = dao.count(stream);

    assertEquals(1, count);

    List<Set<Field<?>>> dtos = dao.findAll(c.stream);

    assertEquals(1, dtos.size());
    assertEquals(11, dtos.get(0).size());

    Set<Field<?>> tuple = new HashSet<>();
    tuple.addAll(c.getFields());
    tuple.add(df);
    tuple.add(cf);

    Assert.assertTrue(dtos.get(0).stream().allMatch(t -> {
      boolean match = tuple.contains(t);
      return match;
    }));

    Assert.assertTrue(tuple.containsAll(dtos.get(0)));

    dtos.get(0).stream().forEach(f -> {

      Assert.assertNotNull(f.getTimestamp());
      Assert.assertEquals(f.getTimestamp(), c.timestamp);
      Assert.assertEquals(f.getStream(), c.stream);

    });
  }

  @Test
  public void test2StoreSecondSetOfFieldsModelData() throws Exception {

    BaseField<BigDecimal> df = new BaseField<>(c.getEventId(), Field.Name.AVE.field() + "-p1", new BigDecimal(102));
    BaseField<BigDecimal> max = new BaseField<BigDecimal>(c.getEventId(), Field.Name.MAX.field() + "-p1", new BigDecimal("11.2"));
    BaseField<BigDecimal> min = new BaseField<BigDecimal>(c.getEventId(), Field.Name.MIN.field() + "-p1", new BigDecimal("1.2"));
    BaseField<BigDecimal> std = new BaseField<BigDecimal>(c.getEventId(), Field.Name.STD.field() + "-p1", new BigDecimal("19.1"));
    BaseField<BigDecimal> sma = new BaseField<BigDecimal>(c.getEventId(), Field.Name.SMA.field() + "-p1", new BigDecimal("0.2"));

    modelDataBus.post(df);
    modelDataBus.post(max);
    modelDataBus.post(min);
    modelDataBus.post(std);
    modelDataBus.post(sma);

    Thread.sleep(1000);

    long count = dao.count(stream);

    assertEquals(count, 1);

    Set<Field<?>> tuple = new HashSet<>();
    List<Set<Field<?>>> dtos = dao.findAll(c.stream);

    tuple.addAll(Sets.newHashSet(df, max, min, std, sma));
    tuple.addAll(c.getFields());
    tuple.add(new BaseField<BigDecimal>(c.getEventId(), "ave", new BigDecimal(100)));
    tuple.add(new BaseField<BigDecimal>(c.getEventId(), "sma", BigDecimal.TEN));

    Assert.assertTrue(tuple.containsAll(dtos.get(0)));
    Assert.assertFalse(dtos.get(0).containsAll(tuple));

    dtos.get(0).stream().forEach(f -> {

      Assert.assertNotNull(f.getTimestamp());
      Assert.assertEquals(f.getTimestamp(), c.timestamp);
      Assert.assertEquals(f.getStream(), c.stream);

    });

  }

  // @Test
  public void testStoreMarker() throws Exception {

    dao.deleteByStream(stream);
    Thread.sleep(2000);

    Notification marker = new Notification(UUID.randomUUID().toString(), "Default");
    marker.addMarker(Markers.QUERY.toString());
    marker.addMarker(Markers.START.toString());
    marker.expect = 100;

    modelDataBus.post(marker);
    modelDataBus.post("test model");

    Thread.sleep(2000);

    long count = dao.count(stream);

    assertEquals(count, 0);
  }
}
