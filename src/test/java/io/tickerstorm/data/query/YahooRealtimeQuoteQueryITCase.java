package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.Quote;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class YahooRealtimeQuoteQueryITCase {

  YahooRealtimeQuoteQuery query;
  DataQueryClient client;
  boolean verified = false;

  @BeforeMethod
  public void setup() {

    client = new DataQueryClient();
    client.historical = new MBassador<MarketData>();
    client.init();
    verified = false;

  }

  @Test
  public void testBasicSymbolQuery() throws Exception {

    query = new YahooRealtimeQuoteQuery("AAPL");
    client.historical.subscribe(new BasicSymbolQuery());
    client.query(query);
    Thread.sleep(2000);
    assertTrue(verified);
  }

  @Listener(references = References.Strong)
  private class BasicSymbolQuery {

    @Handler
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
