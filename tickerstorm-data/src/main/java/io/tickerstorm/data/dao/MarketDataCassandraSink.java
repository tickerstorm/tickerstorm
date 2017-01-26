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

package io.tickerstorm.data.dao;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

@DependsOn(value = {"cassandraSetup"})
@Repository
public class MarketDataCassandraSink extends BaseCassandraSink<MarketDataDto> {

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(MarketDataCassandraSink.class);

  @Override
  protected int batchSize() {
    return 1999;
  }

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  private EventBus historicalBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Autowired
  private MarketDataDao dao;

  @PreDestroy
  public void destroy() {
    super.destroy();
    historicalBus.unregister(this);
  }

  @PostConstruct
  public void init() {
    super.init();
    historicalBus.register(this);
  }

  @Subscribe
  public void onData(MarketData md) {
    batch(MarketDataDto.convert(md));
  }

  protected void persist(Collection<MarketDataDto> data) {

    if (null == data || data.isEmpty())
      return;
    try {

      synchronized (data) {
        logger.debug(
            "Persisting " + data.size() + " records, " + count.addAndGet(data.size()) + " total saved and " + received.get() + " received");

      }

      dao.ingest(data);

      Map<String, Integer> streamCounts = dao.countEntries(data);

      for (Entry<String, Integer> e : streamCounts.entrySet()) {
        Notification marker = new Notification(e.getKey());
        marker.addMarker(Markers.MARKET_DATA.toString());
        marker.addMarker(Markers.SAVE.toString());
        marker.addMarker(Markers.SUCCESS.toString());
        marker.expect = e.getValue();
        notificationsBus.post(marker);
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

}
