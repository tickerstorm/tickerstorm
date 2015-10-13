package io.tickerstorm.entity;

import java.io.Serializable;
import java.util.Set;

public interface Marker extends Serializable {

  public Set<String> getMarkers();

}
