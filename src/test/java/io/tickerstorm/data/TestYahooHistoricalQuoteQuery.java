package io.tickerstorm.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class TestYahooHistoricalQuoteQuery {

  YahooHistoricalQuoteQuery query;
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
    query = new YahooHistoricalQuoteQuery("AAPL").eod().from(new DateTime().minusYears(1))
        .until(new DateTime());

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
