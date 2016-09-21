package io.tickerstorm.common.data.query;

import java.io.Serializable;

import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.Markers;

public interface DataFeedQuery extends Serializable {
  
  public String id();

  default boolean isComplete(Notification marker) {
    return (Markers.is((Marker) marker, Markers.QUERY_END) && ((Notification) marker).expect == 0 && marker.id == this.id());
  }


}
