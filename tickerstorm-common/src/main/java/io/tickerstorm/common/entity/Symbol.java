package io.tickerstorm.common.entity;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Symbol {

  public String symbol;
  public String exchange;

  public Symbol(String symbol) {
    this.symbol = symbol;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public boolean equals(Object obj) {
    return Objects.equal(this, obj);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(symbol, exchange);
  }

}
