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

package io.tickerstorm.data.dao.influxdb;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.influxdb.InfluxDBBatchListener;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kkarski on 4/17/17.
 */
public class BroadcastInfluxDBListener implements InfluxDBBatchListener {

  private static final Logger logger = LoggerFactory.getLogger(BroadcastInfluxDBListener.class);

  private final List<InfluxDBBatchListener> listeners = new CopyOnWriteArrayList<>();

  @Override
  public void onPointBatchWrite(List<Point> points) {

    for (InfluxDBBatchListener l : this.listeners) {
      try {
        l.onPointBatchWrite(points);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public void onException(List<Point> points, Throwable e) {
    for (InfluxDBBatchListener l : this.listeners) {
      try {
        l.onException(points, e);
      } catch (Exception ex) {
        logger.error(ex.getMessage(), ex);
      }
    }
  }

  public void addListener(InfluxDBBatchListener listener) {
    this.listeners.add(listener);
  }

  public void removeListener(InfluxDBBatchListener listener) {
    this.listeners.remove(listener);
  }
}
