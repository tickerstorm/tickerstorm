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

package io.tickerstorm.data.service;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.MarketDataDto;
import io.tickerstorm.data.dao.cassandra.CassandraMarketDataDao;
import io.tickerstorm.data.dao.cassandra.CassandraMarketDataDto;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

@Repository
public class HistoricalDataFeed {

  private static final java.time.format.DateTimeFormatter dateFormat =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));

  private static final Logger logger = LoggerFactory.getLogger(HistoricalDataFeed.class);

  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Autowired
  private EventBus realtimeBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus queryBus;

  @Autowired
  private MarketDataDao dao;

  @Autowired
  private CassandraOperations cassandra;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @PostConstruct
  public void setup() {
    queryBus.register(this);
  }

  @PreDestroy
  public void destroy() {
    queryBus.unregister(this);
  }

  @Subscribe
  public void onDelete(Command delete) {
    if (delete.markers.contains(Markers.MARKET_DATA.toString()) && delete.markers.contains(Markers.DELETE.toString())) {
      dao.deleteByStream(delete.getStream());
      Notification notif = new Notification(delete);
      notif.markers.add(Markers.SUCCESS.toString());
      notificationBus.post(notif);

      logger.debug("Deleted market data per " + delete);
    }
  }

  @Subscribe
  public void onQuery(HistoricalFeedQuery query) {

    logger.debug("Historical feed query received");
    Instant start = query.from.toInstant(ZoneOffset.UTC);
    Instant end = query.until.toInstant(ZoneOffset.UTC);
    Instant date = start;

    List<BigInteger> dates = new ArrayList<>();
    dates.add(new BigInteger(dateFormat.format(date)));

    while (date.compareTo(end) < 0) {
      date = date.plus(1, ChronoUnit.DAYS);
      dates.add(new BigInteger(dateFormat.format(date)));
    }

    for (String s : query.symbols) {

      Select select = QueryBuilder.select().from(keyspace, "marketdata");
      select.where(QueryBuilder.eq("symbol", s.toLowerCase())).and(QueryBuilder.in("date", dates))
          .and(QueryBuilder.eq("type", Bar.TYPE.toLowerCase())).and(QueryBuilder.eq("source", query.source.toLowerCase()))
          .and(QueryBuilder.eq("interval", query.periods.iterator().next()));

      logger.debug("Cassandra query: " + select.toString());
      long startTimer = System.currentTimeMillis();
      List<MarketDataDto> dtos = cassandra.select(select, MarketDataDto.class);
      logger.info("Query took " + (System.currentTimeMillis() - startTimer) + "ms to fetch " + dtos.size() + " results.");

      Notification marker = new Notification(query);
      marker.addMarker(Markers.START.toString());
      marker.expect = dtos.size();
      notificationBus.post(marker);

      startTimer = System.currentTimeMillis();
      dtos.stream().map(d -> {
        return d.toMarketData(query.getStream());
      }).forEach(m -> {
        realtimeBus.post(m);
      });

      marker = new Notification(query);
      marker.addMarker(Markers.END.toString());
      marker.expect = 0;
      notificationBus.post(marker);

      logger.debug("Dispatch historical data feed took " + (System.currentTimeMillis() - startTimer) + "ms");

    }
  }
}
