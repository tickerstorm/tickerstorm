package io.tickerstorm.common.entity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public class Bar extends BaseMarketData {

  public static final String TYPE = "candle";
  public static final String MIN_5_INTERVAL = "5m";
  public static final String MIN_1_INTERVAL = "1m";
  public static final String MIN_10_INTERVAL = "10m";
  public static final String WEEK_INTERVAL = "7d";
  public static final String HOURLY_INTERVAL = "1h";
  public static final String EOD = "EOD";

  public Bar() {}

  public static String parseInterval(String eventId) {

    if (MarketData.parseEventId(eventId).length == 4) {
      return MarketData.parseEventId(eventId)[3];
    }

    return null;
  }

  public Bar(Set<Field<?>> fields) {
    super(fields);

    for (Field<?> f : fields) {

      if (f.getName().equalsIgnoreCase(Field.Name.VOLUME.field()))
        this.volume = (Integer) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.HIGH.field()))
        this.high = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.LOW.field()))
        this.low = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.OPEN.field()))
        this.open = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.CLOSE.field()))
        this.close = (BigDecimal) f.getValue();

      if (f.getName().equalsIgnoreCase(Field.Name.INTERVAL.field()))
        this.interval = (String) f.getValue();
    }
  }

  public Bar(String symbol, String source, Instant timestamp, BigDecimal open, BigDecimal close, BigDecimal high, BigDecimal low,
      String interval, int volume) {
    super(symbol, source, timestamp);
    this.close = close;
    this.open = open;
    this.high = high;
    this.low = low;
    this.interval = interval;
    this.volume = volume;
  }

  public BigDecimal low;

  public BigDecimal high;

  public BigDecimal open;

  public BigDecimal close;

  public Integer volume;

  public String interval;

  public BigDecimal getClose() {
    return close;
  }

  public BigDecimal getHigh() {
    return high;
  }

  public String getInterval() {
    return interval;
  }

  public BigDecimal getLow() {
    return low;
  }

  public BigDecimal getOpen() {
    return open;
  }

  public String getType() {
    return TYPE;
  }

  public void setType(String type) {
    // nothing
  }

  public Integer getVolume() {
    return volume;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((close == null) ? 0 : close.hashCode());
    result = prime * result + ((high == null) ? 0 : high.hashCode());
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
    result = prime * result + ((low == null) ? 0 : low.hashCode());
    result = prime * result + ((open == null) ? 0 : open.hashCode());
    result = prime * result + ((volume == null) ? 0 : volume.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    Bar other = (Bar) obj;
    if (close == null) {
      if (other.close != null)
        return false;
    } else if (!close.equals(other.close))
      return false;
    if (high == null) {
      if (other.high != null)
        return false;
    } else if (!high.equals(other.high))
      return false;
    if (interval == null) {
      if (other.interval != null)
        return false;
    } else if (!interval.equals(other.interval))
      return false;
    if (low == null) {
      if (other.low != null)
        return false;
    } else if (!low.equals(other.low))
      return false;
    if (open == null) {
      if (other.open != null)
        return false;
    } else if (!open.equals(other.open))
      return false;
    if (volume == null) {
      if (other.volume != null)
        return false;
    } else if (!volume.equals(other.volume))
      return false;
    return true;
  }

  public void setClose(BigDecimal close) {
    this.close = close;
  }

  public void setHigh(BigDecimal high) {
    this.high = high;
  }

  public void setInterval(String interval) {
    this.interval = interval;
  }

  public void setLow(BigDecimal low) {
    this.low = low;
  }

  public void setOpen(BigDecimal open) {
    this.open = open;
  }

  public void setVolume(Integer volume) {
    this.volume = volume;
  }

  @Override
  public String toString() {
    String v = super.toString();
    return MoreObjects.toStringHelper(this).addValue(v).add("interval", interval).add("open", open).add("close", close).add("high", high)
        .add("low", low).add("volume", volume).toString();
  }

  /**
   * Format: source|symbol|timestamp|interval
   * 
   * @return
   */
  @Override
  public String getEventId() {
    return new StringBuffer(super.getEventId()).append("|").append(getInterval()).toString();
  }

  @Override
  public Set<Field<?>> getFields() {
    Set<Field<?>> fields = super.getFields();
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.HIGH.field(), high));
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.LOW.field(), low));
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.OPEN.field(), open));
    fields.add(new BaseField<BigDecimal>(getEventId(), Field.Name.CLOSE.field(), close));
    fields.add(new BaseField<Integer>(getEventId(), Field.Name.VOLUME.field(), volume));
    fields.add(new BaseField<String>(getEventId(), Field.Name.INTERVAL.field(), interval));
    return fields;

  }

}
