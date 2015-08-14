package io.tickerstorm.data.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.tickerstorm.data.query.DataQueryClient;
import io.tickerstorm.data.query.YahooChartsDataQuery;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class YahooChartsDataQueryITCase {

  YahooChartsDataQuery query;
  DataQueryClient client;
  boolean verified = false;

  @BeforeMethod
  public void setup() {

    client = new DataQueryClient();
    client.historical = new EventBus();
    client.init();
    verified = false;

  }

  @Test
  public void testBasicSymbolQuery() {

    query = new YahooChartsDataQuery("AAPL");
    client.historical.register(new BasicSymbolQueryVerification());
    client.query(query);
    assertTrue(verified);
  }

  private class BasicSymbolQueryVerification {

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
      assertEquals(c.interval, Candle.MIN_5_INTERVAL);
      verified = true;

    }

  }
}