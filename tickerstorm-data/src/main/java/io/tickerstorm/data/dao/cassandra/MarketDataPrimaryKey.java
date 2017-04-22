/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.data.dao.cassandra;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Date;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

@PrimaryKeyClass
@SuppressWarnings("serial")
public class MarketDataPrimaryKey implements Serializable {

  @PrimaryKeyColumn(name = "symbol", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
  public String symbol;

  @PrimaryKeyColumn(name = "date", ordinal = 1, type = PrimaryKeyType.PARTITIONED, ordering = Ordering.ASCENDING)
  public BigInteger date;

  @PrimaryKeyColumn(name = "type", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
  public String type;

  @PrimaryKeyColumn(name = "source", ordinal = 3, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.ASCENDING)
  public String stream;

  @PrimaryKeyColumn(name = "interval", ordinal = 4, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.ASCENDING)
  public String interval;

  @PrimaryKeyColumn(name = "timestamp", ordinal = 5, type = PrimaryKeyType.CLUSTERED)
  public Date timestamp;

  @PrimaryKeyColumn(name = "hour", ordinal = 6, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.ASCENDING)
  public Integer hour;

  @PrimaryKeyColumn(name = "min", ordinal = 6, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.ASCENDING)
  public Integer min;

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MarketDataPrimaryKey other = (MarketDataPrimaryKey) obj;
    if (date == null) {
      if (other.date != null)
        return false;
    } else if (!date.equals(other.date))
      return false;
    if (hour == null) {
      if (other.hour != null)
        return false;
    } else if (!hour.equals(other.hour))
      return false;
    if (interval == null) {
      if (other.interval != null)
        return false;
    } else if (!interval.equals(other.interval))
      return false;
    if (min == null) {
      if (other.min != null)
        return false;
    } else if (!min.equals(other.min))
      return false;
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
      return false;
    if (symbol == null) {
      if (other.symbol != null)
        return false;
    } else if (!symbol.equals(other.symbol))
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    return true;
  }

  public String getInterval() {
    return interval;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public BigInteger getDate() {
    return date;
  }

  public void setDate(BigInteger date) {
    this.date = date;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public String getStream() {
    return stream;
  }

  public String getSymbol() {
    return symbol;
  }

  public String getType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((date == null) ? 0 : date.hashCode());
    result = prime * result + ((hour == null) ? 0 : hour.hashCode());
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
    result = prime * result + ((min == null) ? 0 : min.hashCode());
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  public void setInterval(String interval) {
    this.interval = interval;
  }

  public void setStream(String source) {
    this.stream = source;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public void setType(String type) {
    this.type = type;
  }
  
  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
