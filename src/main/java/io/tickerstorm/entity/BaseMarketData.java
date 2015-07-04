package io.tickerstorm.entity;

import java.io.Serializable;
import java.time.Instant;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public abstract class BaseMarketData implements MarketData, Serializable {

  public String source;

  public Instant timestamp;

  public String symbol;

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BaseMarketData other = (BaseMarketData) obj;
    if (source == null) {
      if (other.source != null)
        return false;
    } else if (!source.equals(other.source))
      return false;
    if (symbol == null) {
      if (other.symbol != null)
        return false;
    } else if (!symbol.equals(other.symbol))
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    return true;
  }

  public String getSource() {
    return source;
  }

  public String getSymbol() {
    return symbol;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }


  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

}
