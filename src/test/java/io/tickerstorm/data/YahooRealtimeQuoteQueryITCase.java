package io.tickerstorm.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.Quote;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class YahooRealtimeQuoteQueryITCase {

  YahooRealtimeQuoteQuery query;
  DataQueryClient client;
  boolean verified = false;

  @BeforeMethod
  public void setup() {

    client = new DataQueryClient();
    client.bus = new EventBus();
    client.init();
    verified = false;

  }

  @Test
  public void testBasicSymbolQuery() {

    query = new YahooRealtimeQuoteQuery("AAPL");
    client.bus.register(new BasicSymbolQuery());
    client.query(query);
    assertTrue(verified);
  }

  private class BasicSymbolQuery {

    @Subscribe
    public void onEvent(MarketData md) {
      assertEquals(md.getSymbol(), "AAPL");
      assertEquals(md.getSource(), "yahoo");
      assertNotNull(md.getTimestamp());

      Quote c = (Quote) md;

      assertNotNull(c.ask);
      assertNotNull(c.bid);
      assertNotNull(c.askSize);
      assertNotNull(c.bidSize);
      verified = true;
    }

  }

}
