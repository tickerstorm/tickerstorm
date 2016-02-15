package io.tickerstorm.common.entity;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("serial")
public class Quote extends BaseMarketData {

  public static final String TYPE = "quote";

  public Quote() {

  }

  public Quote(Set<Field<?>> fields) {
    super(fields);
    for (Field<?> f : fields) {

      if (f.getName().equalsIgnoreCase(Field.Name.ASK.field()))
        this.ask = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.ASK_SIZE.field()))
        this.askSize = (Integer) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.BID.field()))
        this.bid = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.BID_SIZE.field()))
        this.bidSize = (Integer) f.getValue();
    }
  }

  public BigDecimal getBid() {
    return bid;
  }

  public void setBid(BigDecimal bid) {
    this.bid = bid;
  }

  public BigDecimal getAsk() {
    return ask;
  }

  public void setAsk(BigDecimal ask) {
    this.ask = ask;
  }

  public Integer getAskSize() {
    return askSize;
  }

  public void setAskSize(Integer askSize) {
    this.askSize = askSize;
  }

  public Integer getBidSize() {
    return bidSize;
  }

  public void setBidSize(Integer bidSize) {
    this.bidSize = bidSize;
  }

  public BigDecimal bid;
  public BigDecimal ask;
  public Integer askSize;
  public Integer bidSize;

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((ask == null) ? 0 : ask.hashCode());
    result = prime * result + ((askSize == null) ? 0 : askSize.hashCode());
    result = prime * result + ((bid == null) ? 0 : bid.hashCode());
    result = prime * result + ((bidSize == null) ? 0 : bidSize.hashCode());
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
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    Quote other = (Quote) obj;
    if (ask == null) {
      if (other.ask != null)
        return false;
    } else if (!ask.equals(other.ask))
      return false;
    if (askSize == null) {
      if (other.askSize != null)
        return false;
    } else if (!askSize.equals(other.askSize))
      return false;
    if (bid == null) {
      if (other.bid != null)
        return false;
    } else if (!bid.equals(other.bid))
      return false;
    if (bidSize == null) {
      if (other.bidSize != null)
        return false;
    } else if (!bidSize.equals(other.bidSize))
      return false;
    return true;
  }

  @Override
  public Set<Field<?>> getFields() {
    Set<Field<?>> fields = new HashSet<Field<?>>();
    fields.addAll(super.getFields());
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.ASK.field(), ask));
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.BID.field(), bid));
    fields.add(new BaseField<Integer>(getEventId(), Field.Name.ASK_SIZE.field(), askSize));
    fields.add(new BaseField<Integer>(getEventId(), Field.Name.BID_SIZE.field(), bidSize));
    return fields;
  }
}
