package io.tickerstorm.data.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class MarketDataCassandraSinkITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  private MarketDataCassandraSink sink;

  @Autowired
  private CassandraOperations session;

  @Autowired
  private MarketDataDao dao;

  @AfterMethod
  public void cleanup() {
    session.getSession().execute("TRUNCATE marketdata");
  }

  @Test
  public void storeCandle() throws Exception {

    Candle c = new Candle();
    c.source = "test";
    c.high = BigDecimal.TEN;
    c.low = BigDecimal.ONE;
    c.interval = Candle.MIN_10_INTERVAL;
    c.open = BigDecimal.ZERO;
    c.timestamp = Instant.now();
    c.volume = 10;
    c.symbol = "AAPL";
    sink.onMarketData(c);

    Thread.sleep(5000);

    long count = dao.count();

    assertEquals(count, 1);

    Iterable<MarketDataDto> result = dao.findAll();

    for (MarketDataDto dto : result) {

      assertEquals(dto.close, c.close);
      assertEquals(dto.low, c.low);
      assertEquals(dto.high, c.high);
      assertEquals(dto.open, c.open);
      assertEquals(dto.volume, new BigDecimal(c.volume));
      assertEquals(dto.primarykey.source, c.source);
      assertEquals(dto.primarykey.interval, c.interval);
      assertEquals(dto.primarykey.timestamp, Date.from(c.timestamp));
      assertEquals(dto.primarykey.symbol, c.symbol.toLowerCase());
      assertNotNull(dto.primarykey.date);

      Candle d = (Candle) dto.toMarketData();

      assertEquals(d.close, c.close);
      assertEquals(d.low, c.low);
      assertEquals(d.high, c.high);
      assertEquals(d.open, c.open);
      assertEquals(d.volume, c.volume);
      assertEquals(d.source, c.source);
      assertEquals(d.interval, c.interval);

      assertTrue(d.timestamp.compareTo(c.timestamp) == 0);
      assertNotNull(d.timestamp);
      assertNotNull(c.timestamp);
      assertEquals(d.symbol, c.symbol.toLowerCase());

    }

  }

}
