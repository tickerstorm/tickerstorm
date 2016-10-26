package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;

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

public class DukascopyHistoricalForexQueryITCase extends BaseDataQueryITCase {

  @BeforeMethod
  public void setup() throws Exception {
    verifier = new DownloadGloabForextVerification();
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Dukascopy"));
    super.setup();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"));
    super.tearDown();
  }

  @Test
  public void parseGloabForext() throws Exception {

    Files.copy(new File("./src/test/resources/data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"),
        new File(Locations.FILE_DROP_LOCATION + "/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"));

    Thread.sleep(16000);

    Long daoCount = dao.count("Dukascopy");
    Assert.assertEquals(daoCount, new Long(52860));
    Assert.assertEquals(count.get(), 52860L);

  }

  private class DownloadGloabForextVerification {

    @Subscribe
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), "Dukascopy");
      assertNotNull(md.getTimestamp());

      Bar c = (Bar) md;
      assertNotNull(c.close);
      assertTrue(c.close.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.open);
      assertTrue(c.open.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.low);
      assertTrue(c.low.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.high);
      assertTrue(c.high.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.volume);
      assertEquals(c.interval, Bar.MIN_1_INTERVAL);
      count.incrementAndGet();

    }

  }
}
