package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.util.Set;

public interface Marker extends Serializable {

  public Set<String> getMarkers();

}
