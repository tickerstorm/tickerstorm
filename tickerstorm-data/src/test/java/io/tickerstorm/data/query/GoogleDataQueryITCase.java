package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import io.tickerstorm.data.TestMarketDataServiceConfig;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class GoogleDataQueryITCase extends AbstractTestNGSpringContextTests {

  GoogleDataQuery query;

  @Autowired
  private DataQueryClient client;

  @Qualifier("historical")
  @Autowired
  MBassador<MarketData> bus;

  @Autowired
  private MarketDataDao dao;

  @Autowired
  private CassandraOperations session;

  private AtomicLong count = new AtomicLong(0);

  GoogleDataVerifier verifier;

  @BeforeMethod
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File("./data/Google"));
    verifier = new GoogleDataVerifier();
    bus.subscribe(verifier);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {

    bus.unsubscribe(verifier);
    session.getSession().execute("TRUNCATE marketdata");
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
    count.set(0);
  }

  @Test
  public void downloadGloabForext() throws Exception {

    Long count = dao.count();
    Assert.assertEquals(count, new Long(0));

    query = new GoogleDataQuery("TOL");
    client.query(query);

    count = dao.count();
    assertTrue(count > 0);

    Thread.sleep(3000);// let things clear;
  }

  @Test
  public void testParseGoogleFile() throws Exception {

    Long daoCount = dao.count();
    Assert.assertEquals(daoCount, new Long(0));

    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"),
        new File("./data/Google/TOL.csv"));

    Thread.sleep(10000);

    daoCount = dao.count();
    assertEquals(count.get(), 5792, "Failed message broker count");
    assertEquals(daoCount, new Long(5792), "Failed dao count");

    while (bus.hasPendingMessages()) {
      Thread.sleep(1000);
    }

  }

  @Listener(references = References.Strong)
  private class GoogleDataVerifier {

    @Handler
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getSource(), "Google");
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
      count.getAndIncrement();

    }

  }
}
