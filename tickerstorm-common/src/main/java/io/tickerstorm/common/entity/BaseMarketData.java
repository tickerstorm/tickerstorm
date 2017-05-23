package io.tickerstorm.common.entity;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.tickerstorm.common.entity.Field.Name;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("serial")
public abstract class BaseMarketData implements MarketData, Serializable {

  public Instant timestamp;
  public String symbol;
  public String stream;
  protected Map<String, Field<?>> fields = new HashMap<>();

  public BaseMarketData(Set<Field<?>> fs) {

    Preconditions.checkArgument(fields.containsKey(Field.Name.TIMESTAMP.field()), "Timestamp is required");
    Preconditions.checkArgument(fields.containsKey(Name.SYMBOL.field()), "Symbol is required");
    Preconditions.checkArgument(fields.containsKey(Name.STREAM.field()), "Stream is required");

    for (Field<?> f : fs) {
      fields.put(f.getName(), f);
    }

    this.timestamp = (Instant) fields.get(Field.Name.TIMESTAMP.field()).getValue();
    this.symbol = (String) fields.get(Field.Name.SYMBOL.field()).getValue();
    this.stream = (String) fields.get(Field.Name.STREAM.field()).getValue();

  }

  protected BaseMarketData() {
  }

  public BaseMarketData(String symbol, String stream, Instant timestamp) {
    this.timestamp = timestamp;
    this.symbol = symbol;
    this.stream = stream;
  }

  public void addField(Field<?> f) {
    this.fields.put(f.getName(), f);
  }

  public boolean removeField(String name) {
    return (this.fields.remove(name) != null);
  }

  /**
   * Items will be sorted in chronological order so that the newest event is always first in a
   * collection
   */
  @Override
  public int compareTo(MarketData o) {
    return o.getTimestamp().compareTo(this.getTimestamp());
  }

  public Set<Field<?>> getFields() {

    Set<Field<?>> fields = new HashSet<Field<?>>();
    fields.addAll(this.fields.values());
    fields.add(new BaseField<String>(getEventId(), Field.Name.SYMBOL.field(), symbol));
    fields.add(new BaseField<Instant>(getEventId(), Field.Name.TIMESTAMP.field(), timestamp));

    if (!StringUtils.isEmpty(stream)) {
      fields.add(new BaseField<String>(getEventId(), Field.Name.STREAM.field(), stream));
    } else {
      throw new IllegalArgumentException("Market data event must have stream");
      //fields.add(new BaseField<String>(getEventId(), Field.Name.STREAM.field(), String.class));
    }

    return fields;

  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public String getSymbol() {
    return symbol;
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
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BaseMarketData other = (BaseMarketData) obj;
    if (stream == null) {
      if (other.stream != null) {
        return false;
      }
    } else if (!stream.equals(other.stream)) {
      return false;
    }
    if (symbol == null) {
      if (other.symbol != null) {
        return false;
      }
    } else if (!symbol.equals(other.symbol)) {
      return false;
    }
    if (timestamp == null) {
      if (other.timestamp != null) {
        return false;
      }
    } else if (!timestamp.equals(other.timestamp)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("symbol", symbol).add("timestamp", timestamp).add("stream", stream).toString();
  }


}
