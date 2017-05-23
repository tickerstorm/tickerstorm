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

package io.tickerstorm.data.feed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import io.tickerstorm.data.dao.influxdb.InfluxMarketDataDao;
import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class HistoricalDataFeedITCase {

  Notification start;
  Notification end;
  AtomicInteger count = new AtomicInteger(0);
  int expCount = 388;
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Autowired
  private EventBus realtimeBus;
  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;
  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus queryBus;
  @Autowired
  private InfluxMarketDataDao dao;

  private DefaultResourceLoader loader = new DefaultResourceLoader();

  @Before
  public void dataSetup() throws Exception {
    HistoricalDataFeedVerifier verifier = new HistoricalDataFeedVerifier();
    realtimeBus.register(verifier);
    notificationsBus.register(verifier);

    dao.newDelete().delete();
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Google"));
    Files.copy(loader.getResource("classpath:/data/Google/TOL.csv").getFile(), new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
    Thread.sleep(3000);

  }

  @After
  public void cleanup() {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
  }

  @Test
  public void testSimpleCandleQuery() throws Exception {

    assertEquals(count.get(), 0L);

    HistoricalFeedQuery query = new HistoricalFeedQuery("google", "google", "TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
    query.periods.add(Bar.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    queryBus.post(query);

    Thread.sleep(500);

    assertEquals(expCount, count.get());

    assertNotNull(start);
    assertNotNull(end);
    assertEquals(start.id, query.id);
    assertEquals(end.id, query.id);
    assertEquals(Integer.valueOf(expCount), start.expect);
    assertEquals(Integer.valueOf(0), end.expect);

  }


  public class HistoricalDataFeedVerifier {

    @Subscribe
    public void onNotification(Notification md) {

      if (md.getMarkers().contains(Markers.QUERY.toString()) && md.getMarkers()
          .contains(Markers.START.toString())) {
        start = md;
      }

      if (md.getMarkers().contains(Markers.QUERY.toString()) && md.getMarkers()
          .contains(Markers.END.toString())) {
        end = md;
      }

    }

    @Subscribe
    public void onMarketData(Bar c) {

      assertNotNull(c.getSymbol());
      assertEquals(c.getStream(), "google");
      assertNotNull(c.getTimestamp());

      assertNotNull(c.close);
      assertTrue(c.close.longValue() > 0);
      assertNotNull(c.open);
      assertTrue(c.open.longValue() > 0);
      assertNotNull(c.low);
      assertTrue(c.low.longValue() > 0);
      assertNotNull(c.high);
      assertTrue(c.high.longValue() > 0);
      assertNotNull(c.volume);
      assertTrue(c.volume.longValue() > 0);
      assertEquals(c.interval, Bar.MIN_1_INTERVAL);
      count.incrementAndGet();
    }


  }

}
