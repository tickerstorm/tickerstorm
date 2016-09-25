package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;

import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.MarketData;

public class StooqHistoricalForexQueryITCase extends BaseDataQueryITCase {

  @BeforeMethod
  public void setup() throws Exception {
    verifier = new DownloadGloabForextVerification();
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Stooq"));
    super.setup();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Stooq/5_world_txt.zip"));
    super.tearDown();
  }

  @Test(enabled = false)
  public void parseGloabForext() throws Exception {

    Files.copy(new File("./src/test/resources/data/Stooq/5_world_txt.zip"),
        new File(Locations.FILE_DROP_LOCATION + "/Stooq/5_world_txt.zip"));

    Thread.sleep(27000);

    Long daoCount = dao.count();
    assertEquals(daoCount.longValue(), 62355L);
    assertEquals(count.get(), 62355L);

  }

  @Test(enabled = false)
  public void downloadGloabForext() throws Exception {

    StooqHistoricalForexQuery query = new StooqHistoricalForexQuery().currencies().min5();
    client.query(query);

    Thread.sleep(20000);

    Long count = dao.count();
    assertTrue(count > 0);

  }

  private class DownloadGloabForextVerification {

    @Subscribe
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), "Stooq");
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
      assertEquals(c.interval, Candle.MIN_5_INTERVAL);
      count.incrementAndGet();

    }

  }
}
