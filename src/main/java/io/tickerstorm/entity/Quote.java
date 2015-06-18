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

  public BigInteger getAskSize() {
    return askSize;
  }

  public void setAskSize(BigInteger askSize) {
    this.askSize = askSize;
  }

  public BigInteger getBidSize() {
    return bidSize;
  }

  public void setBidSize(BigInteger bidSize) {
    this.bidSize = bidSize;
  }

  public BigDecimal bid;
  public BigDecimal ask;
  public BigInteger askSize;
  public BigInteger bidSize;

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), bid, ask);
  }

  @Override
  public String getType() {
     return TYPE;
  }

}
