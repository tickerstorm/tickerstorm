package io.tickerstorm.data.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class MarketDataCassandraSinkITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  private CassandraOperations session;

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private EventBus historicalBus;

  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Autowired
  private EventBus realtimeBus;

  @Autowired
  private MarketDataDao dao;

  private RealtimeBusListener listener;

  @BeforeMethod
  public void cleanup() {
    listener = new RealtimeBusListener();
    realtimeBus.register(listener);
    dao.deleteByStream("MarketDataCassandraSinkITCase".toLowerCase());
  }

  @Test
  public void storeCandle() throws Exception {

    Bar c = new Bar();
    c.high = BigDecimal.TEN;
    c.low = BigDecimal.ONE;
    c.interval = Bar.MIN_10_INTERVAL;
    c.open = BigDecimal.ZERO;
    c.timestamp = Instant.now();
    c.volume = 10;
    c.symbol = "AAPL";
    c.stream = "MarketDataCassandraSinkITCase".toLowerCase();
    historicalBus.post(c);

    Thread.sleep(5000);

    long count = dao.count("MarketDataCassandraSinkITCase".toLowerCase());

    assertEquals(count, 1);
    Assert.assertEquals(listener.count, 1);

    Stream<MarketDataDto> result = dao.findAll(c.stream);

    result.forEach(dto -> {

      assertEquals(dto.close, c.close);
      assertEquals(dto.low, c.low);
      assertEquals(dto.high, c.high);
      assertEquals(dto.open, c.open);
      assertEquals(dto.volume, new BigDecimal(c.volume));
      assertEquals(dto.primarykey.stream, c.stream.toLowerCase());
      assertEquals(dto.primarykey.interval, c.interval);
      assertEquals(dto.primarykey.timestamp, Date.from(c.timestamp));
      assertEquals(dto.primarykey.symbol, c.symbol.toLowerCase());
      assertNotNull(dto.primarykey.date);

      Bar d = (Bar) dto.toMarketData(c.stream);

      assertEquals(d.close, c.close);
      assertEquals(d.low, c.low);
      assertEquals(d.high, c.high);
      assertEquals(d.open, c.open);
      assertEquals(d.volume, c.volume);
      assertEquals(d.stream, c.stream.toLowerCase());
      assertEquals(d.interval, c.interval);

      assertTrue(d.timestamp.compareTo(c.timestamp) == 0);
      assertNotNull(d.timestamp);
      assertNotNull(c.timestamp);
      assertEquals(d.symbol, c.symbol.toLowerCase());

    });

  }

  public class RealtimeBusListener {

    public int count = 0;

    @Subscribe
    public void onMarketData(MarketData md) {
      count++;
    }

  }

}
