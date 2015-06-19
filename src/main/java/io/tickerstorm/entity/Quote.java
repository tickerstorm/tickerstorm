package io.tickerstorm.entity;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.springframework.data.cassandra.mapping.Table;

import com.google.common.base.Objects;

@SuppressWarnings("serial")
public class Quote extends BaseMarketData {

  public static final String TYPE = "quote";

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

  public BigDecimal getAskSize() {
    return askSize;
  }

  public void setAskSize(BigDecimal askSize) {
    this.askSize = askSize;
  }

  public BigDecimal getBidSize() {
    return bidSize;
  }

  public void setBidSize(BigDecimal bidSize) {
    this.bidSize = bidSize;
  }

  public BigDecimal bid;
  public BigDecimal ask;
  public BigDecimal askSize;
  public BigDecimal bidSize;

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

  public void setType(String type){
    //nothing
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
}
