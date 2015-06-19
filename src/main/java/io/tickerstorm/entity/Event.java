package io.tickerstorm.entity;

import org.joda.time.DateTime;

public interface Event {

  public DateTime getTimestamp();
  public String getSource();
  public String getType();

}
