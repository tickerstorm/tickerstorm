package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public abstract class BaseMarketData implements MarketData, Serializable {

  public String source;
  public Instant timestamp;
  public String symbol;
  public String stream;
  protected Map<String, Field<?>> fields = new HashMap<>();

  public BaseMarketData(Set<Field<?>> fs) {

    for (Field<?> f : fs) {
      fields.put(f.getName(), f);
    }

    this.source = (String) fields.get(Field.Name.SOURCE.field()).getValue();
    this.timestamp = (Instant) fields.get(Field.Name.TIMESTAMP.field()).getValue();
    this.symbol = (String) fields.get(Field.Name.SYMBOL.field()).getValue();
    this.stream = (String) fields.get(Field.Name.STREAM.field()).getValue();
  }

  protected BaseMarketData() {}

  public BaseMarketData(String symbol, String source, Instant timestamp) {
    this.source = source;
    this.timestamp = timestamp;
    this.symbol = symbol;
  }

  public BaseMarketData(String symbol, String source, String stream, Instant timestamp) {
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.source = source;
    this.stream = stream;
  }

  public Set<Field<?>> getFields() {

    Set<Field<?>> fields = new HashSet<Field<?>>();
    fields.add(new BaseField<String>(getEventId(), Field.Name.SYMBOL.field(), symbol));
    fields.add(new BaseField<Instant>(getEventId(), Field.Name.TIMESTAMP.field(), timestamp));
    fields.add(new BaseField<String>(getEventId(), Field.Name.SOURCE.field(), source));

    if (!StringUtils.isEmpty(stream))
      fields.add(new BaseField<String>(getEventId(), Field.Name.STREAM.field(), stream));
    else
      fields.add(new BaseField<String>(getEventId(), Field.Name.STREAM.field(), String.class));

    return fields;

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
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
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
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

  public String getStream() {
    return stream;
  }

  public String getSymbol() {
    return symbol;
  }


  public Instant getTimestamp() {
    return timestamp;
  }

  public void setSource(String source) {
    this.source = source;
  }


  public void setStream(String stream) {
    this.stream = stream;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("symbol", symbol).add("source", source).add("timestamp", timestamp).add("stream", stream)
        .toString();
  }



}
