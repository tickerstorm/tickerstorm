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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDao;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class TestCassandraModelDataDao {

  private final Bar c =
      new Bar("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @Before
  public void init() {
    c.stream = "TestModelDataDto";
  }

  @Autowired
  private CassandraModelDataDao dao;

  @Test
  public void testSelectBars() throws Exception {

    List<Bar> bars = TestDataFactory
        .buildCandles(5, "goog", "TestModelDataDto", new BigDecimal("34.4354"));
    Set<CassandraModelDataDto> dtos = CassandraModelDataDto.convertBars(bars);
    dao.ingest(dtos);

    bars = TestDataFactory.buildCandles(2, "tol", "TestModelDataDto2", new BigDecimal("12.243"));
    dtos = CassandraModelDataDto.convertBars(bars);
    dao.ingest(dtos);

    long count = dao.count("TestModelDataDto");
    org.junit.Assert.assertEquals(5, count);

    count = dao.count("TestModelDataDto2");
    org.junit.Assert.assertEquals(2, count);

    Stream<CassandraModelDataDto> dtoss = dao.findAll("TestModelDataDto");
    org.junit.Assert.assertEquals(5, dtoss.count());

    dtoss = dao.findAll("TestModelDataDto2");
    org.junit.Assert.assertEquals(2, dtoss.count());

    dao.deleteByStream("TestModelDataDto");

    count = dao.count("TestModelDataDto");
    org.junit.Assert.assertEquals(0, count);

    count = dao.count("TestModelDataDto2");
    org.junit.Assert.assertEquals(2, count);

  }

  @Test
  public void testSerializeDeserializeModelData() {

    Set<Field<?>> fields = c.getFields();
    assertEquals(fields.size(), 9);

    Set<CassandraModelDataDto> dtos = CassandraModelDataDto.convertFields(fields);
    assertNotNull(dtos);
    assertEquals(dtos.size(), 9);

    assertTrue(dtos.stream().allMatch(d -> {

      return fields.containsAll(d.asFields());

    }));

  }

  @After
  public void cleanup() throws Exception {
    dao.deleteByStream("TestModelDataDto");
    dao.deleteByStream("TestModelDataDto2");
  }
}
