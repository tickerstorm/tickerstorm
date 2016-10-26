package io.tickerstorm.data.feed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;

import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class HistoricalDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Autowired
  private EventBus realtimeBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus queryBus;

  Notification start;
  Notification end;

  @Autowired
  private CassandraOperations session;

  AtomicInteger count = new AtomicInteger(0);
  int expCount = 778;

  @BeforeClass
  public void dataSetup() throws Exception {
    HistoricalDataFeedVerifier verifier = new HistoricalDataFeedVerifier();
    realtimeBus.register(verifier);
    notificationsBus.register(verifier);

    session.getSession().execute("TRUNCATE marketdata");
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
    Thread.sleep(10000);
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
  }

  @Test
  public void testSimpleCandleQuery() throws Exception {

    assertEquals(count.get(), 0L);

    HistoricalFeedQuery query = new HistoricalFeedQuery("google", "google", new String[] {"TOL"});
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
    query.periods.add(Bar.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    queryBus.post(query);

    Thread.sleep(10000);

    assertEquals(count.get(), expCount);

    assertNotNull(start);
    assertNotNull(end);
    assertEquals(start.id, query.id);
    assertEquals(end.id, query.id);
    assertEquals(start.expect, Integer.valueOf(expCount));
    assertEquals(end.expect, Integer.valueOf(0));

  }



  public class HistoricalDataFeedVerifier {

    @Subscribe
    public void onNotification(Notification md) {

      if (md.getMarkers().contains(Markers.QUERY.toString()) && md.getMarkers().contains(Markers.START.toString()))
        start = (Notification) md;

      if (md.getMarkers().contains(Markers.QUERY.toString()) && md.getMarkers().contains(Markers.END.toString()))
        end = (Notification) md;

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
