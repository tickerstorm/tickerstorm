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

package io.tickerstorm.common.reactive;

import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Marker;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import io.tickerstorm.common.entity.Event;

@SuppressWarnings("serial")
public class Notification implements Event, Marker, Serializable {

  public String type = "notification";
  public Integer expect = null;
  public String stream;
  public Instant eventTime = Instant.now();
  public Map<String, String> properties = new HashMap<>();
  public Set<String> markers = new HashSet<>();

  public final String id;

  public Map<String, String> getProperties() {
    return properties;
  }

  protected void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public Notification(String id, String stream) {
    this.id = id;
    this.stream = stream;
  }

  public Notification(Command comm) {
    this.id = comm.id;
    this.stream = comm.getStream();
    this.type = comm.getType();
    this.markers.addAll(comm.markers);
  }

  public Notification(String stream) {
    this.id = UUID.randomUUID().toString();
    this.stream = stream;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((eventTime == null) ? 0 : eventTime.hashCode());
    result = prime * result + ((expect == null) ? 0 : expect.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((markers == null) ? 0 : markers.hashCode());
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
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
    Notification other = (Notification) obj;
    if (eventTime == null) {
      if (other.eventTime != null)
        return false;
    } else if (!eventTime.equals(other.eventTime))
      return false;
    if (expect == null) {
      if (other.expect != null)
        return false;
    } else if (!expect.equals(other.expect))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (markers == null) {
      if (other.markers != null)
        return false;
    } else if (!markers.equals(other.markers))
      return false;
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
      return false;
    return true;
  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  @Override
  public Set<String> getMarkers() {
    return markers;
  }

  public void addMarker(String marker) {
    markers.add(marker);
  }

  public String getType() {
    return type;
  }

  public boolean is(String marker) {
    return markers.contains(marker);
  }

  @Override
  public Instant getTimestamp() {
    return eventTime;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
