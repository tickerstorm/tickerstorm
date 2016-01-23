package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public abstract class BaseMarketData implements MarketData, Serializable {

  public String source;
  public Instant timestamp;
  public String symbol;
  public String stream;

  public String getStream() {
    return stream;
  }

  public BaseMarketData() {}

  public BaseMarketData(String symbol, String source, String stream, Instant timestamp) {
    this.source = source;
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.stream = stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public BaseMarketData(String symbol, String source, Instant timestamp) {
    this.source = source;
    this.symbol = symbol;
    this.timestamp = timestamp;
  }

  public BaseMarketData(Field<?>[] fields) {

    this.source = fields[0].getSource();
    this.timestamp = fields[0].getTimestamp();
    this.symbol = fields[0].getSymbol();

  }

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
    return MoreObjects.toStringHelper(this).add("symbol", symbol).add("source", source).add("timestamp", timestamp).add("stream", stream)
        .toString();
  }

  public Set<Field<?>> getFields() {

    Set<Field<?>> fields = new HashSet<Field<?>>();
    fields.add(new CategoricalField(symbol, timestamp, symbol, Field.SYMBOL, source));
    fields.add(new CategoricalField(symbol, timestamp, timestamp.toString(), Field.TIMESTAMP, source));
    fields.add(new CategoricalField(symbol, timestamp, source, Field.SOURCE, source));
    fields.add(new CategoricalField(symbol, timestamp, stream, Field.TIMESTAMP, source));
    return fields;

  }



}
