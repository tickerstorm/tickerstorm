package io.tickerstorm.entity;

import java.time.Instant;

public abstract class BaseField<T> implements Field<T> {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((currency == null) ? 0 : currency.hashCode());
    result = prime * result + ((field == null) ? 0 : field.hashCode());
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
    if (currency == null) {
      if (other.currency != null)
        return false;
    } else if (!currency.equals(other.currency))
      return false;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
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

  public String currency;
  public final String symbol;
  public final Instant timestamp;
  public final String field;
  public final T value;

  public BaseField(String symbol, Instant timestamp, String currency, String field, T value) {
    this.currency = currency;
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.field = field;
    this.value = value;

  }

  public BaseField(String symbol, Instant timestamp, String field, T value) {
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.field = field;
    this.value = value;
  }

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public String getSymbol() {
    return symbol;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getName() {
    return field;
  }

  public T getValue() {
    return value;
  }



}
