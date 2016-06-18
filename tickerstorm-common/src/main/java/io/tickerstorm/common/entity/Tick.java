package io.tickerstorm.common.entity;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("serial")
public class Tick extends BaseMarketData {

  public Tick() {
    // TODO Auto-generated constructor stub
  }

  public static final String TYPE = "tick";

  public Tick(Set<Field<?>> fields) {
    super(fields);
    for (Field<?> f : fields) {

      if (f.getName().equalsIgnoreCase(Field.Name.PRICE.field()))
        this.price = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.QUANTITY.field()))
        this.quantity = (BigDecimal) f.getValue();

    }
  }

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
    fields.addAll(super.getFields());
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.PRICE.field(), price));
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.QUANTITY.field(), quantity));
    return fields;
  }

}
