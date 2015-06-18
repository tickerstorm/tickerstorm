package io.tickerstorm.dao;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.Quote;
import io.tickerstorm.entity.Tick;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.dozer.DozerBeanMapper;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.data.cassandra.mapping.Table;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

@Table("marketdata")
@SuppressWarnings("serial")
public class MarketDataDto implements Serializable {

  public static final DateTimeFormatter formatter = ISODateTimeFormat.date();
  public static final DozerBeanMapper mapper = new DozerBeanMapper();

  static {
    mapper.setMappingFiles(Lists.newArrayList("dozer.xml"));
  }

  public BigDecimal getHigh() {
    return high;
  }

  public void setHigh(BigDecimal high) {
    this.high = high;
  }

  @org.springframework.data.cassandra.mapping.PrimaryKey
  public PrimaryKey primarykey;

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
  
  public String getExchange() {
    return exchange;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public BigDecimal getQuantity() {
    return quantity;
  }

  public void setQuantity(BigDecimal quantity) {
    this.quantity = quantity;
  }

  public Map<String, Object> properties = new HashMap<String, Object>();

  public PrimaryKey getPrimarykey() {
    return primarykey;
  }

  public void setPrimarykey(PrimaryKey primarykey) {
    this.primarykey = primarykey;
  }

  public BigDecimal getAsk() {
    return ask;
  }

  public void setAsk(BigDecimal ask) {
    this.ask = ask;
  }

  public BigDecimal getBid() {
    return bid;
  }

  public void setBid(BigDecimal bid) {
    this.bid = bid;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public BigDecimal getAskSize() {
    return askSize;
  }

  public void setAskSize(BigDecimal askSize) {
    this.askSize = askSize;
  }

  public BigDecimal getBidSize() {
    return bidSize;
  }

  public void setBidSize(BigDecimal bidSize) {
    this.bidSize = bidSize;
  }

  public BigDecimal getVolume() {
    return volume;
  }

  public void setVolume(BigDecimal volume) {
    this.volume = volume;
  }

  public BigDecimal getClose() {
    return close;
  }

  public void setClose(BigDecimal close) {
    this.close = close;
  }

  public BigDecimal getOpen() {
    return open;
  }

  public void setOpen(BigDecimal open) {
    this.open = open;
  }

  public BigDecimal getLow() {
    return low;
  }

  public void setLow(BigDecimal low) {
    this.low = low;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public static MarketDataDto convert(MarketData data) {

    MarketDataDto dto = new MarketDataDto();
    PrimaryKey key = new PrimaryKey();
    try {
      mapper.map(data, dto);
      mapper.map(data, key);
      key.date = data.getTimestamp().toString(ISODateTimeFormat.date());
      key.timestamp = data.getTimestamp().toString(ISODateTimeFormat.dateTime());
      dto.primarykey = key;
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return dto;

  }

  public MarketData toMarketData() {

    MarketData c = null;

    try {
      if (Candle.TYPE.equals(primarykey.type)) {
        c = mapper.map(this, Candle.class);
      } else if (Quote.TYPE.equals(primarykey.type)) {
        c = mapper.map(this, Quote.class);
      } else if (Tick.TYPE.equals(primarykey.type)) {
        c = mapper.map(this, Tick.class);
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return c;

  }

}
