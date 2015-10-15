package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

public class GoogleDataQueryITCase extends BaseDataQueryITCase {

  GoogleDataQuery query;

  @BeforeMethod
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File("./data/Google"));
    verifier = new GoogleDataVerifier();
    super.setup();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
    super.tearDown();
  }

  @Test
  public void downloadGloabForext() throws Exception {

    Long daoCount = dao.count();
    Assert.assertEquals(daoCount, new Long(0));

    query = new GoogleDataQuery("TOL");
    client.query(query);

    daoCount = dao.count();
    assertTrue(daoCount > 0);

    Thread.sleep(3000);// let things clear;
  }

  @Test
  public void testParseGoogleFile() throws Exception {

    Long daoCount = dao.count();
    Assert.assertEquals(daoCount, new Long(0));

    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File("./data/Google/TOL.csv"));

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
