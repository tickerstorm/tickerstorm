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

  @PrimaryKeyColumn(name = "modelName", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
  public String modelName;

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
    result = prime * result + ((modelName == null) ? 0 : modelName.hashCode());
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
    if (modelName == null) {
      if (other.modelName != null)
        return false;
    } else if (!modelName.equals(other.modelName))
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    return true;
  }

  public String getModelName() {
    return modelName;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setDate(Integer date) {
    this.date = date;
  }

  public void setModelName(String modelName) {
    this.modelName = modelName;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }
}
