package io.tickerstorm.common.command;

import java.io.Serializable;
import java.util.Set;

public interface Marker extends Serializable {

  public Set<String> getMarkers();

}
