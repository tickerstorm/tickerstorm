package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.springframework.data.cassandra.mapping.Table;

import com.google.common.base.Throwables;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Quote;
import io.tickerstorm.common.entity.Tick;

@Table("marketdata")
@SuppressWarnings("serial")
public class MarketDataDto implements Serializable {

  public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("uuuuMMdd");

  public static MarketDataDto convert(MarketData data) {

    // Ignore ticks or quotes
    if (Bar.TYPE.equals(data.getType())) {

      MarketDataDto dto = new MarketDataDto();
      MarketDataPrimaryKey key = new MarketDataPrimaryKey();
      try {

        Bar c = (Bar) data;
        dto.close = c.close;
        dto.open = c.open;
        dto.volume = BigDecimal.valueOf(c.volume);
        dto.high = c.high;
        dto.low = c.low;

        LocalDateTime dt = LocalDateTime.ofInstant(data.getTimestamp(), ZoneOffset.UTC);

        key.symbol = data.getSymbol().toLowerCase();
        key.stream = data.getStream().toLowerCase();
        key.date = new BigInteger(dateFormatter.format(dt));
        key.hour = dt.getHour();
        key.min = dt.getMinute();
        key.timestamp = Date.from(data.getTimestamp());
        key.type = data.getType();
        key.interval = c.getInterval().toLowerCase();
        dto.primarykey = key;

      } catch (Exception e) {
        Throwables.propagate(e);
      }

      return dto;
    }

    return null;

  }

  @org.springframework.data.cassandra.mapping.PrimaryKey
  public MarketDataPrimaryKey primarykey;

  public BigDecimal ask;
  public BigDecimal bid;
  public BigDecimal price;
  public BigDecimal askSize;
  public BigDecimal bidSize;
  public BigDecimal volume;
  public BigDecimal close;
  public BigDecimal open;
  public BigDecimal high;
  public BigDecimal low;
  public BigDecimal quantity;
  public String exchange;
  public Map<String, Object> properties = new HashMap<String, Object>();

  public BigDecimal getAsk() {
    return ask;
  }

  public BigDecimal getAskSize() {
    return askSize;
  }

  public BigDecimal getBid() {
    return bid;
  }

  public BigDecimal getBidSize() {
    return bidSize;
  }

  public BigDecimal getClose() {
    return close;
  }

  public String getExchange() {
    return exchange;
  }

  public BigDecimal getHigh() {
    return high;
  }

  public BigDecimal getLow() {
    return low;
  }

  public BigDecimal getOpen() {
    return open;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public MarketDataPrimaryKey getPrimarykey() {
    return primarykey;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public BigDecimal getQuantity() {
    return quantity;
  }

  public BigDecimal getVolume() {
    return volume;
  }

  public void setAsk(BigDecimal ask) {
    this.ask = ask;
  }

  public void setAskSize(BigDecimal askSize) {
    this.askSize = askSize;
  }

  public void setBid(BigDecimal bid) {
    this.bid = bid;
  }

  public void setBidSize(BigDecimal bidSize) {
    this.bidSize = bidSize;
  }

  public void setClose(BigDecimal close) {
    this.close = close;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public void setHigh(BigDecimal high) {
    this.high = high;
  }

  public void setLow(BigDecimal low) {
    this.low = low;
  }

  public void setOpen(BigDecimal open) {
    this.open = open;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public void setPrimarykey(MarketDataPrimaryKey primarykey) {
    this.primarykey = primarykey;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public void setQuantity(BigDecimal quantity) {
    this.quantity = quantity;
  }

  public void setVolume(BigDecimal volume) {
    this.volume = volume;
  }

  public MarketData toMarketData(String stream) {

    MarketData c = null;

    try {
      if (Bar.TYPE.equals(primarykey.type)) {

        c = new Bar(this.primarykey.symbol, stream, this.primarykey.timestamp.toInstant(), this.open, this.close,
            this.high, this.low, this.primarykey.interval, this.volume.intValue());

      } else if (Quote.TYPE.equals(primarykey.type)) {

        c = new Quote(this.primarykey.symbol, stream, this.primarykey.timestamp.toInstant(), this.ask,
            this.askSize.intValue(), this.bid, this.bidSize.intValue());

      } else if (Tick.TYPE.equals(primarykey.type)) {

        c = new Tick(this.primarykey.symbol, stream, this.primarykey.timestamp.toInstant(), this.price, this.quantity);

      }

    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return c;

  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

}
