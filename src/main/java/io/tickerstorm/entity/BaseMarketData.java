package io.tickerstorm.entity;

import java.io.Serializable;

import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

@SuppressWarnings("serial")
public abstract class BaseMarketData implements MarketData, Serializable {

  public void setSource(String source) {
    this.source = source;
  }

  public void setTimestamp(DateTime timestamp) {
    this.timestamp = timestamp;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public String source;
  public DateTime timestamp;
  public String symbol;
  public String exchange;

  @Override
  public boolean equals(Object obj) {
    return Objects.equal(this, obj);
  }

  public String getExchange() {
    return exchange;
  }

  public String getSource() {
    return source;
  }

  public String getSymbol() {
    return symbol;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(source, timestamp, symbol);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

}
