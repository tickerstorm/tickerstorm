package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;

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

  private class BasicSymbolQueryVerification {

    @Subscribe
    public void onEvent(MarketData md) {

      assertEquals(md.getSymbol(), "AAPL");
      assertEquals(md.getStream(), "Yahoo");
      assertNotNull(md.getTimestamp());

      Bar c = (Bar) md;

      assertNotNull(c.close);
      assertNotNull(c.open);
      assertNotNull(c.low);
      assertNotNull(c.high);
      assertNotNull(c.volume);
      assertEquals(c.interval, Bar.MIN_5_INTERVAL);
      count.incrementAndGet();

    }

  }
}
