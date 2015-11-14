package io.tickerstorm.common.entity;

import java.time.Instant;

import com.google.common.base.MoreObjects;

public abstract class BaseField<T> implements Field<T> {

  public String getInterval() {
    return interval;
  }

  public final String symbol;
  public final Instant timestamp;
  public String interval;
  public final String field;
  public final T value;
  public final String source;

  public BaseField(String symbol, Instant timestamp, String field, String source, T value, String interval) {
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.field = field;
    this.value = value;
    this.source = source;
    this.interval = interval;
  }

  public String getName() {
    return field;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
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
    BaseField other = (BaseField) obj;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
      return false;
    if (interval == null) {
      if (other.interval != null)
        return false;
    } else if (!interval.equals(other.interval))
      return false;
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
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

  public String getSource() {
    return source;
  }

  public String getSymbol() {
    return symbol;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public T getValue() {
    return value;
  }
  
  protected static String[] parseFields(String value){
    String[] vals = value.split("=");
    String[] fields = vals[0].split("_");

    for (int i = 0; i < fields.length; i++) {
      if (fields[i].equalsIgnoreCase("null"))
        fields[i] = null;
    }
    
    return fields;
  }
  
  protected static String parseValue(String value){
    String[] vals = value.split("=");
    return vals[1];
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", this.field).add("source", this.source).add("symbol", this.symbol)
        .add("timestamp", this.timestamp).add("value", this.value).add("interval", interval).toString();
  }



}
