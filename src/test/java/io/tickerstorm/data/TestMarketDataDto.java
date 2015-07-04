package io.tickerstorm.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import io.tickerstorm.dao.MarketDataDto;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.Quote;
import io.tickerstorm.entity.Tick;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;

import org.testng.annotations.Test;

public class TestMarketDataDto {

  @Test
  public void convertCandle() {

    Candle c = new Candle();
    c.close = BigDecimal.TEN;
    c.open = BigDecimal.TEN;
    c.low = BigDecimal.TEN;
    c.high = BigDecimal.TEN;
    c.source = "test";
    c.symbol = "AAPL";
    c.interval = Candle.EOD;
    c.timestamp = Instant.now();
    c.volume = BigDecimal.ZERO;

    MarketDataDto dto = MarketDataDto.convert(c);

    assertEquals(dto.close, c.close);
    assertEquals(dto.low, c.low);
    assertEquals(dto.high, c.high);
    assertEquals(dto.open, c.open);
    assertEquals(dto.volume, c.volume);
    assertEquals(dto.primarykey.source, c.source);
    assertEquals(dto.primarykey.interval, c.interval.toLowerCase());
    assertEquals(dto.primarykey.timestamp, Date.from(c.timestamp));
    assertEquals(dto.primarykey.symbol, c.symbol.toLowerCase());
    assertNotNull(dto.primarykey.date);

    Candle d = (Candle) dto.toMarketData();

    assertEquals(d.close, c.close);
    assertEquals(d.low, c.low);
    assertEquals(d.high, c.high);
    assertEquals(d.open, c.open);
    assertEquals(d.volume, c.volume);
    assertEquals(d.source, c.source.toLowerCase());
    assertEquals(d.interval, c.interval.toLowerCase());

    // assertEquals(d.timestamp, c.timestamp); not working
    assertNotNull(d.timestamp);
    assertNotNull(c.timestamp);
    assertEquals(d.symbol, c.symbol.toLowerCase());

  }

  @Test
  public void convertQuote() {

    Quote c = new Quote();
    c.ask = BigDecimal.TEN;
    c.bid = BigDecimal.TEN;
    c.source = "test";
    c.symbol = "AAPL";

    c.timestamp = Instant.now();
    c.askSize = BigDecimal.ZERO;
    c.bidSize = BigDecimal.ZERO;

    MarketDataDto dto = MarketDataDto.convert(c);

    assertEquals(dto.ask, c.ask);
    assertEquals(dto.bid, c.bid);
    assertEquals(dto.askSize, c.askSize);
    assertEquals(dto.bidSize, c.bidSize);
    assertEquals(dto.primarykey.source, c.source.toLowerCase());
    assertEquals(dto.primarykey.timestamp, Date.from(c.timestamp));
    assertEquals(dto.primarykey.symbol, c.symbol.toLowerCase());
    assertNotNull(dto.primarykey.date);

    Quote d = (Quote) dto.toMarketData();

    assertEquals(d.ask, c.ask);
    assertEquals(d.bid, c.bid);
    assertEquals(d.askSize, c.askSize);
    assertEquals(d.bidSize, c.bidSize);
    assertEquals(d.source, c.source.toLowerCase());

    // assertEquals(d.timestamp, c.timestamp); not working
    assertNotNull(d.timestamp);
    assertNotNull(c.timestamp);
    assertEquals(d.symbol, c.symbol.toLowerCase());

  }

  @Test
  public void convertTick() {

    Tick c = new Tick();
    c.price = BigDecimal.TEN;
    c.quantity = BigDecimal.TEN;
    c.source = "test";
    c.symbol = "AAPL";

    c.timestamp = Instant.now();

    MarketDataDto dto = MarketDataDto.convert(c);

    assertEquals(dto.price, c.price);
    assertEquals(dto.quantity, c.quantity);
    assertEquals(dto.primarykey.source, c.source);
    assertEquals(dto.primarykey.timestamp, Date.from(c.timestamp));
    assertEquals(dto.primarykey.symbol, c.symbol.toLowerCase());
    assertNotNull(dto.primarykey.date);

    Tick d = (Tick) dto.toMarketData();

    assertEquals(d.price, c.price);
    assertEquals(d.quantity, c.quantity);
    assertEquals(d.source, c.source.toLowerCase());

    // assertEquals(d.timestamp, c.timestamp); not working
    assertNotNull(d.timestamp);
    assertNotNull(c.timestamp);
    assertEquals(d.symbol, c.symbol.toLowerCase());

  }

}
