package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import java.time.LocalDateTime;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class YahooHistoricalQuoteQueryITCase {

  YahooHistoricalQuoteQuery query;
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
  public void testBasicSymbolQuery() {
    query =
        new YahooHistoricalQuoteQuery("AAPL").eod().from(LocalDateTime.now().minusYears(1))
            .until(LocalDateTime.now());

    client.historical.subscribe(new BasicSymbolQuery());
    client.query(query);
    assertTrue(verified);

  }

  @Listener(references = References.Strong)
  private class BasicSymbolQuery {

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
      assertEquals(c.interval, Candle.EOD);
      verified = true;
    }

  }

}
