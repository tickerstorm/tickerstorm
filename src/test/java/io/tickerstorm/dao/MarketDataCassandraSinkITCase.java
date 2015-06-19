package io.tickerstorm.dao;

import static org.testng.Assert.*;
import io.tickerstorm.TickerStormAppConfig;
import io.tickerstorm.entity.Candle;

import java.math.BigDecimal;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@ContextConfiguration(classes = { TickerStormAppConfig.class })
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
  public void storeCandle() {

    Candle c = new Candle();
    c.source = "test";
    c.high = BigDecimal.TEN;
    c.low = BigDecimal.ONE;
    c.interval = Candle.MIN_10_INTERVAL;
    c.open = BigDecimal.ZERO;
    c.timestamp = new DateTime().withZone(DateTimeZone.forOffsetHours(-7));
    c.volume = BigDecimal.TEN;
    c.symbol = "AAPL";
    sink.onMarketData(c);

    long count = dao.count();

    assertEquals(count, 1);
    
    Iterable<MarketDataDto> result = dao.findAll();
    
    for(MarketDataDto dto : result){
     
      assertEquals(dto.close, c.close);
      assertEquals(dto.low, c.low);
      assertEquals(dto.high, c.high);
      assertEquals(dto.open, c.open);
      assertEquals(dto.volume, c.volume);
      assertEquals(dto.primarykey.source, c.source);
      assertEquals(dto.primarykey.interval, c.interval);
      assertEquals(dto.primarykey.timestamp, c.timestamp.toDate());
      assertEquals(dto.primarykey.symbol, c.symbol);
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
      assertEquals(d.symbol, c.symbol);      
      
    }

  }

}
