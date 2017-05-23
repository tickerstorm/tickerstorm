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
import static org.junit.Assert.assertTrue;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class TestInfluxModelDataDao {

  private final Bar c =
      new Bar("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", new BigDecimal(1000));
  @Autowired
  private InfluxModelDataDao dao;

  @Test
  public void testSelectBars() throws Exception {

    c.stream = "TestModelDataDto";

    List<Bar> bars = TestDataFactory.buildCandles(5, "goog", c.stream, new BigDecimal("34.4354"));
    dao.ingestMarketData((List) bars);

    bars = TestDataFactory.buildCandles(2, "tol", c.stream + 2, new BigDecimal("12.243"));
    dao.ingestMarketData((List) bars);

    Thread.sleep(500);

    long count = dao.count(c.stream);
    org.junit.Assert.assertEquals(5, count);

    count = dao.count(c.stream + 2);
    org.junit.Assert.assertEquals(2, count);

    List<Set<Field<?>>> dtoss = dao.findAll(c.stream);
    org.junit.Assert.assertEquals(45, dtoss.stream().reduce(new HashSet<Field<?>>(), (s, e) -> {
      s.addAll(e);
      return s;
    }).size());

    dtoss = dao.findAll(c.stream + 2);
    org.junit.Assert.assertEquals(18, dtoss.stream().reduce(new HashSet<Field<?>>(), (s, e) -> {
      s.addAll(e);
      return s;
    }).size());

    dao.deleteByStream(c.stream);

    count = dao.count(c.stream);
    org.junit.Assert.assertEquals(0, count);

    count = dao.count(c.stream + 2);
    org.junit.Assert.assertEquals(2, count);

  }

  @Test
  public void testSerializeDeserializeModelData() {

    Set<Field<?>> fields = c.getFields();
    assertEquals(fields.size(), 9);

    InfluxModelDataDto dtos = InfluxModelDataDto.convert(fields);
    assertNotNull(dtos);
    assertEquals(dtos.getFields().size(), 9);
    assertEquals(dtos.getPoints().size(), 5);
    assertTrue(fields.containsAll(dtos.getFields()));

  }

  @Test
  public void testSelectHeaders() throws Exception {
    dao.ingestMarketData(c);

    Thread.sleep(200);
    Set<String> headers = dao.newSelect(c.stream).selectHeaders(c.stream);

    Assert.assertFalse(headers.isEmpty());
    Assert.assertTrue(c.getFields().stream().allMatch(f -> {
      return headers.contains(f.getName());
    }));
  }

  @After
  public void cleanup() throws Exception {
    dao.deleteByStream(c.stream);
    dao.deleteByStream(c.stream + 2);
  }
}
