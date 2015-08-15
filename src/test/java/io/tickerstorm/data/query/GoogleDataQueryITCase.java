package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.data.MarketDataServiceConfig;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;

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

  Object verifier;

  boolean verified = false;

  @BeforeMethod
  public void setup() throws Exception {
    verified = false;
    FileUtils.forceMkdir(new File("./data/Google"));
  }

  @AfterMethod
  public void tearDown() {
    bus.subscribe(verifier);
    session.getSession().execute("TRUNCATE marketdata");
    FileUtils.deleteQuietly(new File("./data/Google/TOL.csv"));
  }

  @Test
  public void downloadGloabForext() throws Exception {

    verifier = new GoogleDataVerifier();
    bus.subscribe(verifier);

    query = new GoogleDataQuery("TOL");
    client.query(query);

    Thread.sleep(5000);
    assertTrue(verified);

    Long count = dao.count();
    assertTrue(count > 0);

  }

  @Test
  public void testParseGoogleFile() throws Exception {

    verifier = new GoogleDataVerifier();
    bus.subscribe(verifier);

    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(
        "./data/Google/TOL.csv"));

    Thread.sleep(20000);
    assertTrue(verified);

    Long count = dao.count();
    assertTrue(count > 0);
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
      verified = true;

    }

  }
}
