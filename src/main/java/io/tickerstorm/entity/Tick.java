package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

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
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tick other = (Tick) obj;
    if (price == null) {
      if (other.price != null)
        return false;
    } else if (!price.equals(other.price))
      return false;
    if (quantity == null) {
      if (other.quantity != null)
        return false;
    } else if (!quantity.equals(other.quantity))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((price == null) ? 0 : price.hashCode());
    result = prime * result + ((quantity == null) ? 0 : quantity.hashCode());
    return result;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public void setType(String type) {
    // nothing
  }

  @Override
  public Set<Field<?>> getFields() {
    Set<Field<?>> fields = new HashSet<Field<?>>();
    fields.add(new ContinousField(symbol, timestamp, "USD", price, Field.PRICE));
    fields.add(new ContinousField(symbol, timestamp, "USD", quantity, Field.QUANTITY));
    return fields;
  }

}
