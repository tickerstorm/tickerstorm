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

import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;

import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;

public class GoogleDataQueryITCase extends BaseDataQueryITCase {

  GoogleDataQuery query;

  @BeforeMethod
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Google"));
    verifier = new GoogleDataVerifier();
    super.setup();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
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

    Long daoCount = dao.count("Google");
    Assert.assertEquals(daoCount, new Long(0));

    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));

    Thread.sleep(12000);

    daoCount = dao.count("Google");
    assertEquals(count.get(), 5792, "Failed message broker count");
    assertEquals(daoCount, new Long(5792), "Failed dao count");

    Thread.sleep(5000);

  }


  private class GoogleDataVerifier {

    @Subscribe
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), "Google");
      assertNotNull(md.getTimestamp());

      Bar c = (Bar) md;
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
      count.getAndIncrement();

    }

  }
}
