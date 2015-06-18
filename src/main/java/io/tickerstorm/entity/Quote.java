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
    return Objects.hashCode(super.hashCode(), bid, ask);
  }

  @Override
  public String getType() {
     return TYPE;
  }

}
