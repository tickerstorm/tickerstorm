package io.tickerstorm.dao;

import java.io.Serializable;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

@PrimaryKeyClass
@SuppressWarnings("serial")
public class PrimaryKey implements Serializable {

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getInterval() {
    return interval;
  }

  public void setInterval(String interval) {
    this.interval = interval;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @PrimaryKeyColumn(name = "symbol", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
  public String symbol;

  @PrimaryKeyColumn(name = "date", ordinal = 1, type = PrimaryKeyType.PARTITIONED, ordering = Ordering.DESCENDING)
  public String date;

  @PrimaryKeyColumn(name = "type", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
  public String type;

  @PrimaryKeyColumn(name = "source", ordinal = 3, type = PrimaryKeyType.CLUSTERED)
  public String source;

  @PrimaryKeyColumn(name = "interval", ordinal = 4, type = PrimaryKeyType.CLUSTERED)
  public String interval;

  @PrimaryKeyColumn(name = "timestamp", ordinal = 5, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
  public String timestamp;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public boolean equals(Object obj) {
    return com.google.common.base.Objects.equal(this, obj);
  }

  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(symbol, timestamp, date, interval, source);
  }
}