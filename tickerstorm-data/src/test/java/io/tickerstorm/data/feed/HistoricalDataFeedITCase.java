package io.tickerstorm.data.feed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.data.query.HistoricalFeedQuery;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.MarketDataMarker;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class HistoricalDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  @Qualifier("notification")
  @Autowired
  private MBassador<Serializable> notificationsBus;

  @Qualifier("query")
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

  MarketDataMarker start;
  MarketDataMarker end;

  @Autowired
  private CassandraOperations session;

  AtomicInteger count = new AtomicInteger(0);
  int expCount = 778;

  @BeforeClass
  public void dataSetup() throws Exception {
    FileUtils.forceMkdir(new File("./data/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File("./data/Google/TOL.csv"));
    Thread.sleep(5000);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
    session.getSession().execute("TRUNCATE marketdata");
  }

  @BeforeMethod
  public void setup() {
    HistoricalDataFeedVerifier verifier = new HistoricalDataFeedVerifier();
    realtimeBus.subscribe(verifier);
    notificationsBus.subscribe(verifier);
  }

  @Test
  public void testSimpleCandleQuery() throws Exception {

    assertEquals(count.get(), 0L);

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    queryBus.publish(query);

    Thread.sleep(3000);

    assertEquals(count.get(), expCount);

    assertNotNull(start);
    assertNotNull(end);
    assertEquals(start.id, query.id);
    assertEquals(end.id, query.id);
    assertEquals(start.expect, Integer.valueOf(expCount));
    assertEquals(end.expect, Integer.valueOf(0));

  }


  @Listener(references = References.Strong)
  public class HistoricalDataFeedVerifier {

    @Handler
    public void onMarketData(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getSource(), "google");
      assertNotNull(md.getTimestamp());

      if (MarketDataMarker.class.isAssignableFrom(md.getClass())) {

        if (((MarketDataMarker) md).getMarkers().contains(Markers.QUERY_START.toString()))
          start = (MarketDataMarker) md;

        if (((MarketDataMarker) md).getMarkers().contains(Markers.QUERY_END.toString()))
          end = (MarketDataMarker) md;

      } else if (Candle.class.isAssignableFrom(md.getClass())) {

        Candle c = (Candle) md;
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
        assertEquals(c.interval, Candle.MIN_1_INTERVAL);
        count.incrementAndGet();
      }
    }

  }

}
