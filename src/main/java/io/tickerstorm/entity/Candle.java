package io.tickerstorm.entity;

import java.math.BigDecimal;

import com.google.common.base.Objects;

@SuppressWarnings("serial")
public class Candle extends BaseMarketData {

  public BigDecimal getHigh() {
    return high;
  }

  public void setHigh(BigDecimal high) {
    this.high = high;
  }

  public static final String TYPE = "candle";

  public static final String MIN_5_INTERVAL = "5m";
  public static final String MIN_1_INTERVAL = "1m";
  public static final String MIN_10_INTERVAL = "10m";
  public static final String WEEK_INTERVAL = "7d";
  public static final String HOURLY_INTERVAL = "1h"; 
  public static final String MONTH_INTERVAL = "1mon";
  public BigDecimal getLow() {
    return low;
  }

  public void setLow(BigDecimal low) {
    this.low = low;
  }

  public BigDecimal getOpen() {
    return open;
  }

  public void setOpen(BigDecimal open) {
    this.open = open;
  }

  public BigDecimal getClose() {
    return close;
  }

  public void setClose(BigDecimal close) {
    this.close = close;
  }

  public BigDecimal getVolume() {
    return volume;
  }

  public void setVolume(BigDecimal volume) {
    this.volume = volume;
  }

  public String getInterval() {
    return interval;
  }

  public void setInterval(String interval) {
    this.interval = interval;
  }

  public static final String EOD = "EOD";

  public BigDecimal low;
  public BigDecimal high;
  public BigDecimal open;
  public BigDecimal close;
  public BigDecimal volume;
  public String interval;

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), low, high, open, close, volume, interval);
  }

  @Override
  public String getType() {
     return TYPE;
  }
}
