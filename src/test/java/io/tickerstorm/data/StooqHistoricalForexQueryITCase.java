package io.tickerstorm.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.TickerStormConfig;
import io.tickerstorm.dao.MarketDataDao;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;

@ContextConfiguration(classes = { TickerStormConfig.class })
public class StooqHistoricalForexQueryITCase extends AbstractTestNGSpringContextTests {

  StooqHistoricalForexQuery query;

  @Autowired
  private DataQueryClient client;

  @Autowired
  EventBus bus;
  
  @Autowired
  private CassandraOperations session;

  Object verifier;

  boolean verified = false;

  @BeforeMethod
  public void setup() throws Exception {
    verified = false;
    FileUtils.forceMkdir(new File("./data/Stooq"));
  }

  @AfterMethod
  public void tearDown() {
    bus.unregister(verifier);
    session.getSession().execute("TRUNCATE marketdata");
  }

  @Test
  public void parseGloabForext() throws Exception {

    verifier = new DownloadGloabForextVerification();
    bus.register(verifier);

    Files.copy(new File("./src/test/resources/data/Stooq/5_world_txt.zip"), new File("./data/Stooq/5_world_txt.zip"));

    Thread.sleep(20000);
    assertTrue(verified);

  }

  @Test
  public void downloadGloabForext() throws Exception {

    verifier = new DownloadGloabForextVerification();
    bus.register(verifier);

    query = new StooqHistoricalForexQuery().withCurrencies().int5min();
    client.query(query);

    Thread.sleep(20000);
    assertTrue(verified);

  }

  private class DownloadGloabForextVerification {

    @Subscribe
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getSource(), "Stooq");
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
      verified = true;

    }

  }
}
