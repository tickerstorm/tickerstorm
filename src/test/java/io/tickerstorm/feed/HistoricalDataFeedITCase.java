package io.tickerstorm.feed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.DataLoadSchedulerConfig;
import io.tickerstorm.dao.MarketDataDao;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;

@ContextConfiguration(classes = { DataLoadSchedulerConfig.class })
public class HistoricalDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("query")
  @Autowired
  private EventBus bus;

  @Autowired
  private HistoricalDataFeed feed;

  @Autowired
  private CassandraOperations session;

  boolean verified = false;
  int count = 0;

  @Autowired
  private MarketDataDao dao;

  @BeforeClass
  public void dataSetup() throws Exception {
    bus.register(new HistoricalDataFeedVerifier());
    FileUtils.forceMkdir(new File("./data/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File("./data/Google/TOL.csv"));
    Thread.sleep(10000);

    Long count = dao.count();
    assertTrue(count > 0);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
    session.getSession().execute("TRUNCATE marketdata");
  }

  @Test
  public void testSimpleCandleQuery() throws Exception {

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 20, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);

    feed.onQuery(query);
    assertEquals(3096, count);

  }

  public class HistoricalDataFeedVerifier {

    @Subscribe
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
      count++;
    }

  }

}
