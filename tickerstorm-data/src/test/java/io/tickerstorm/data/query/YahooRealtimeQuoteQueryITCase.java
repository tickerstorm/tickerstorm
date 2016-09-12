package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Quote;

public class YahooRealtimeQuoteQueryITCase extends BaseDataQueryITCase {

  YahooRealtimeQuoteQuery query;

  @BeforeMethod
  public void setup() throws Exception {
    verifier = new BasicSymbolQuery();
    super.setup();
  }

  @Test
  public void testBasicSymbolQuery() throws Exception {

    query = new YahooRealtimeQuoteQuery("AAPL");
    client.query(query);

    Thread.sleep(2000);

    assertTrue(count.get() > 0);
  }

  private class BasicSymbolQuery {

    @Subscribe
    public void onEvent(MarketData md) {
      assertEquals(md.getSymbol(), "AAPL");
      assertEquals(md.getStream(), "Yahoo");
      assertNotNull(md.getTimestamp());

      Quote c = (Quote) md;

      assertNotNull(c.ask);
      assertNotNull(c.bid);
      assertNotNull(c.askSize);
      assertNotNull(c.bidSize);
      count.incrementAndGet();
    }

  }

}
