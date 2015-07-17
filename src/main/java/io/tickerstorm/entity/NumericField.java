package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.time.Instant;

public abstract class NumericField {

  public String currency;
  public final String symbol;
  public final Instant timestamp;
  public final BigDecimal amount;
  public final String field;
  public String interval;

  public NumericField(String symbol, Instant timestamp, String currency, BigDecimal amount, String field, String interval) {
    this.currency = currency;
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.amount = amount;
    this.field = field;
    this.interval = interval;
  }
  
  public NumericField(String symbol, Instant timestamp, String currency, BigDecimal amount, String field) {
    this.currency = currency;
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.amount = amount;
    this.field = field;    
  }

  public NumericField(String symbol, Instant timestamp, BigDecimal quantty, String field, String interval) {
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.amount = quantty;
    this.field = field;
    this.interval = interval;
  }

  public NumericField(String symbol, Instant timestamp, BigDecimal quantty, String field) {
    this.symbol = symbol;
    this.timestamp = timestamp;
    this.amount = quantty;
    this.field = field;
  }

  public String getCurrency() {
    return currency;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((amount == null) ? 0 : amount.hashCode());
    result = prime * result + ((currency == null) ? 0 : currency.hashCode());
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
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
    NumericField other = (NumericField) obj;
    if (amount == null) {
      if (other.amount != null)
        return false;
    } else if (!amount.equals(other.amount))
      return false;
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
    if (interval == null) {
      if (other.interval != null)
        return false;
    } else if (!interval.equals(other.interval))
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

  public String getSymbol() {
    return symbol;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public String getField() {
    return field;
  }

  public String getInterval() {
    return interval;
  }
}
