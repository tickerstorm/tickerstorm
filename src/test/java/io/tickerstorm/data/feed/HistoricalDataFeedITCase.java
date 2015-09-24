package io.tickerstorm.data.feed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.data.MarketDataServiceConfig;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

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

@DirtiesContext
@ContextConfiguration(classes = {MarketDataServiceConfig.class})
public class HistoricalDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  @Qualifier("query")
  @Autowired
  private MBassador<HistoricalFeedQuery> queryBus;

  @Autowired
  private CassandraOperations session;

  boolean verified = false;
  AtomicInteger count = new AtomicInteger(0);
  long expCount = 778;

  @Autowired
  private MarketDataDao dao;

  @BeforeClass
  public void dataSetup() throws Exception {
    realtimeBus.subscribe(new HistoricalDataFeedVerifier());
    FileUtils.forceMkdir(new File("./data/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(
        "./data/Google/TOL.csv"));
    Thread.sleep(10000);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
    session.getSession().execute("TRUNCATE marketdata");
  }

  @BeforeMethod
  public void setup() {
    count = new AtomicInteger(0);
    verified = false;
  }

  @Test
  public void testSimpleCandleQuery() throws Exception {

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    queryBus.post(query).asynchronously();

    Thread.sleep(3000);
    assertEquals(count.get(), expCount);
    assertTrue(verified);
  }


  @Listener(references = References.Strong)
  public class HistoricalDataFeedVerifier {

    @Handler
    public void onMarketData(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getSource(), "google");
      assertNotNull(md.getTimestamp());

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
      verified = true;
      count.incrementAndGet();
    }

  }

}
