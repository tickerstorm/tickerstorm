package io.tickerstorm.dao;

import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.Quote;
import io.tickerstorm.entity.Tick;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.dozer.DozerBeanMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.springframework.data.cassandra.mapping.Table;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

@Table("marketdata")
@SuppressWarnings("serial")
public class MarketDataDto implements Serializable {

  public static final org.joda.time.format.DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyyMMdd");
  public static final DozerBeanMapper mapper = new DozerBeanMapper();

  static {
    mapper.setMappingFiles(Lists.newArrayList("dozer.xml"));
  }

  public static MarketDataDto convert(MarketData data) {

    MarketDataDto dto = new MarketDataDto();
    PrimaryKey key = new PrimaryKey();
    try {

      mapper.map(data, dto);
      mapper.map(data, key);
      key.date = data.getTimestamp().withZone(DateTimeZone.UTC).toString(dateFormatter);
      dto.primarykey = key;
      dto.primarykey.symbol = dto.primarykey.symbol.toLowerCase();
      dto.primarykey.source = dto.primarykey.source.toLowerCase();
      dto.primarykey.interval = dto.primarykey.interval.toLowerCase();
      dto.primarykey.timestamp = new DateTime(dto.primarykey.timestamp).withZone(DateTimeZone.UTC).toDate();

    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return dto;

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

  public PrimaryKey getPrimarykey() {
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

  public void setPrimarykey(PrimaryKey primarykey) {
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
