package io.tickerstorm.entity;

import java.math.BigDecimal;

import com.google.common.base.Objects;

@SuppressWarnings("serial")
public class Tick extends BaseMarketData {

  public static final String TYPE = "tick";

  public BigDecimal getQuantity() {
    return quantity;
  }

  public void setQuantity(BigDecimal quantity) {
    this.quantity = quantity;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public BigDecimal quantity;
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
