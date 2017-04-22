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

package io.tickerstorm.data.dao.cassandra;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDao;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
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
public class ModelDataCassandraSinkITCase {

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  @Autowired
  private CassandraModelDataDao dao;

  private final Instant instant = Instant.now();
  private final String symbol = "goog";
  private final String stream = "ModelDataCassandraSinkITCase".toLowerCase();
  private Bar c;

  @Before
  public void cleanup() throws Exception {
    dao.deleteByStream(stream);
    Thread.sleep(5000);
    c = new Bar(symbol, stream, this.instant, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
  }

  @Test
  public void test1StoreModelData() throws Exception {

    BaseField<Integer> df = new BaseField<Integer>(c.getEventId(), "ave", 100);
    BaseField<BigDecimal> cf = new BaseField<BigDecimal>(c.getEventId(), "sma", BigDecimal.TEN);

    modelDataBus.post(c);
    modelDataBus.post(df);
    modelDataBus.post(cf);

    Thread.sleep(5000);

    long count = dao.count(stream);

    assertEquals(count, 1);


    Set<CassandraModelDataDto> dtos = new HashSet<>();
    dao.findAll(c.stream).forEach(d -> {
      dtos.add(d);
    });

    assertEquals(dtos.size(), 1);

    Set<Field<?>> tuple = new HashSet<>();
    tuple.addAll(c.getFields());
    tuple.add(df);
    tuple.add(cf);

    Assert.assertTrue(dtos.stream().allMatch(d -> {

      return (tuple.containsAll(d.asFields()));

    }));

    dtos.stream().forEach(d -> {

      Assert.assertNotNull(d.primarykey);
      Assert.assertNotNull(d.primarykey.date);
      Assert.assertNotNull(d.primarykey.timestamp);
      Assert.assertEquals(d.primarykey.timestamp, Date.from(c.timestamp));
      Assert.assertEquals(d.primarykey.stream, c.stream);

    });
  }

  @Test
  public void test2StoreSecondSetOfFieldsModelData() throws Exception {

    BaseField<Integer> df = new BaseField<Integer>(c.getEventId(), Field.Name.AVE.field() + "-p1", 102);
    BaseField<BigDecimal> max = new BaseField<BigDecimal>(c.getEventId(), Field.Name.MAX.field() + "-p1", new BigDecimal("11.2"));
    BaseField<BigDecimal> min = new BaseField<BigDecimal>(c.getEventId(), Field.Name.MIN.field() + "-p1", new BigDecimal("1.2"));
    BaseField<BigDecimal> std = new BaseField<BigDecimal>(c.getEventId(), Field.Name.STD.field() + "-p1", new BigDecimal("19.1"));
    BaseField<BigDecimal> sma = new BaseField<BigDecimal>(c.getEventId(), Field.Name.SMA.field() + "-p1", new BigDecimal("0.2"));

    modelDataBus.post(df);
    modelDataBus.post(max);
    modelDataBus.post(min);
    modelDataBus.post(std);
    modelDataBus.post(sma);

    Thread.sleep(5000);

    long count = dao.count(stream);

    assertEquals(count, 1);

    Set<Field<?>> tuple = new HashSet<>();
    Set<CassandraModelDataDto> dtos = new HashSet<>();

    dao.findAll(c.stream).forEach(d -> {
      dtos.add(d);
    });

    tuple.addAll(Sets.newHashSet(df, max, min, std, sma));
    tuple.addAll(c.getFields());
    tuple.add(new BaseField<Integer>(c.getEventId(), "ave", 100));
    tuple.add(new BaseField<BigDecimal>(c.getEventId(), "sma", BigDecimal.TEN));

    Assert.assertTrue(dtos.stream().allMatch(d -> {

      return (tuple.containsAll(d.asFields()));

    }));

    dtos.stream().forEach(d -> {

      Assert.assertNotNull(d.primarykey);
      Assert.assertNotNull(d.primarykey.date);
      Assert.assertNotNull(d.primarykey.timestamp);
      Assert.assertEquals(d.primarykey.timestamp, Date.from(c.timestamp));
      Assert.assertEquals(d.primarykey.stream, c.stream);

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
