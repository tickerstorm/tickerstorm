package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

public class YahooChartsDataQueryITCase extends BaseDataQueryITCase {

  YahooChartsDataQuery query;

  @BeforeMethod
  public void setup() throws Exception {
    verifier = new BasicSymbolQueryVerification();
    super.setup();
  }

  @Test
  public void testBasicSymbolQuery() throws Exception {

    query = new YahooChartsDataQuery("AAPL");
    client.query(query);

    Thread.sleep(3000);

    assertTrue(count.get() > 0);
  }

  @Listener(references = References.Strong)
  private class BasicSymbolQueryVerification {

    @Handler
    public void onEvent(MarketData md) {

      assertEquals(md.getSymbol(), "AAPL");
      assertEquals(md.getSource(), "yahoo");
      assertNotNull(md.getTimestamp());

      Candle c = (Candle) md;

      assertNotNull(c.close);
      assertNotNull(c.open);
      assertNotNull(c.low);
      assertNotNull(c.high);
      assertNotNull(c.volume);
      assertEquals(c.interval, Candle.MIN_5_INTERVAL);
      count.incrementAndGet();

    }

  }
}
