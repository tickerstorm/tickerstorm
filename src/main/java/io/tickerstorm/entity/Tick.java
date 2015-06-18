package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.springframework.data.cassandra.mapping.Table;

import com.google.common.base.Objects;

@SuppressWarnings("serial")
public class Tick extends BaseMarketData {

  public static final String TYPE = "tick";

  public BigInteger getQuantity() {
    return quantity;
  }

  public void setQuantity(BigInteger quantity) {
    this.quantity = quantity;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public BigInteger quantity;
  public BigDecimal price;

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), quantity, price);
  }

  @Override
  public String getType() {
    return TYPE;
  }

}
