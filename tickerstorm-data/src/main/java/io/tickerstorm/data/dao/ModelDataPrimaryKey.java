package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.util.Date;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

@PrimaryKeyClass
@SuppressWarnings("serial")
public class ModelDataPrimaryKey implements Serializable {

  @PrimaryKeyColumn(name = "stream", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
  public String stream;

  @PrimaryKeyColumn(name = "date", ordinal = 1, type = PrimaryKeyType.PARTITIONED, ordering = Ordering.DESCENDING)
  public Integer date;

  @PrimaryKeyColumn(name = "timestamp", ordinal = 2, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
  public Date timestamp;

  public Integer getDate() {
    return date;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((date == null) ? 0 : date.hashCode());
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ModelDataPrimaryKey other = (ModelDataPrimaryKey) obj;
    if (date == null) {
      if (other.date != null)
        return false;
    } else if (!date.equals(other.date))
      return false;
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    return true;
  }

  public String getStream() {
    return stream;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setDate(Integer date) {
    this.date = date;
  }

  public void setStream(String modelName) {
    this.stream = modelName;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }
}
