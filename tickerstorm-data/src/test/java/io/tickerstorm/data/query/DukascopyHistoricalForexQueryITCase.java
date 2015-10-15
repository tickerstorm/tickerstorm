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

import com.google.common.io.Files;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

public class DukascopyHistoricalForexQueryITCase extends BaseDataQueryITCase {

  @BeforeMethod
  public void setup() throws Exception {
    verifier = new DownloadGloabForextVerification();
    FileUtils.forceMkdir(new File("./data/Dukascopy"));
    super.setup();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File("./data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"));
    super.tearDown();
  }

  @Test
  public void parseGloabForext() throws Exception {

    Files.copy(new File("./src/test/resources/data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"),
        new File("./data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"));

    Thread.sleep(15000);

    Long daoCount = dao.count();
    Assert.assertEquals(daoCount, new Long(52860));
    Assert.assertEquals(count.get(), 52860L);

  }

  @Listener(references = References.Strong)
  private class DownloadGloabForextVerification {

    @Handler
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getSource(), "Dukascopy");
      assertNotNull(md.getTimestamp());

      Candle c = (Candle) md;
      assertNotNull(c.close);
      assertTrue(c.close.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.open);
      assertTrue(c.open.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.low);
      assertTrue(c.low.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.high);
      assertTrue(c.high.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.volume);
      assertEquals(c.interval, Candle.MIN_1_INTERVAL);
      count.incrementAndGet();

    }

  }
}
