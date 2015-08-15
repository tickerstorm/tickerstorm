package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.data.DirectoryMonitor;
import io.tickerstorm.data.MarketDataServiceConfig;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.math.BigDecimal;

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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

@DirtiesContext
@ContextConfiguration(classes = {MarketDataServiceConfig.class})
public class DukascopyHistoricalForexQueryITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  private DataQueryClient client;

  @Autowired
  private MarketDataDao dao;

  @Qualifier("historical")
  @Autowired
  MBassador<MarketData> bus;

  @Autowired
  private CassandraOperations session;

  @Autowired
  private DirectoryMonitor monitor;

  Object verifier;

  boolean verified = false;

  @BeforeMethod
  public void setup() throws Exception {
    verified = false;
    FileUtils.forceMkdir(new File("./data/Dukascopy"));
  }

  @AfterMethod
  public void tearDown() {
    bus.subscribe(verifier);
    session.getSession().execute("TRUNCATE marketdata");
  }

  @Test
  public void parseGloabForext() throws Exception {

    verifier = new DownloadGloabForextVerification();
    bus.subscribe(verifier);

    Files
        .copy(
            new File(
                "./src/test/resources/data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"),
            new File("./data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"));

    Thread.sleep(60000);
    assertTrue(verified);

    Long count = dao.count();
    assertTrue(count > 0);

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
      verified = true;

    }

  }
}
