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

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Quote;
import io.tickerstorm.common.entity.Tick;
import io.tickerstorm.common.eventbus.Destinations;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * Created by kkarski on 4/10/17.
 */
@Repository
public class InfluxMarketDataDao {

  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(InfluxMarketDataDao.class);


  @Value("${influx.keyspace}")
  private String keyspace;
  @Autowired
  private InfluxDB influx;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  public void ingest(Collection<MarketData> dtos) {

    List<InfluxMarketDataDto> dto = InfluxMarketDataDto.convert(dtos);
    for (InfluxMarketDataDto d : dto) {
      influx.write(keyspace, "autogen", d.getPoint());
    }
  }

  public void persist(MarketData md) {

    InfluxMarketDataDto dto = InfluxMarketDataDto.convert(md);
    Point p = dto.getPoint();
    influx.write(keyspace, "autogen", p);
  }

  public List<MarketData> findAll(String stream) {
    return new Select().bySource(stream).byType(Bar.TYPE).select();
  }

  public Delete newDelete() {
    return new Delete();
  }

  public Select newSelect() {
    return new Select();
  }

  public Count newCount(String type) {
    return new Count(type);
  }

  public class Select {

    private String where = "";
    private String stream;

    Select() {
    }

    public Select asStream(String stream) {
      this.stream = stream;
      return this;
    }

    public Select bySource(String source) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "source='" + source.toLowerCase() + "'";
      return this;
    }

    public Select byType(String type) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "type='" + type.toLowerCase() + "'";
      return this;
    }

    public Select byInterval(String interval) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "interval='" + interval.toLowerCase() + "'";
      return this;
    }

    public Select bySymbol(String... symbol) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      final StringBuffer symbols = new StringBuffer();

      Arrays.stream(symbol).forEach(s -> {
        if (!StringUtils.isEmpty(symbols)) {
          symbols.append(" OR ");
        }

        symbols.append("symbol='" + s.toLowerCase() + "'");
      });

      where += symbols.toString();

      return this;
    }

    public Select between(Instant from, Instant to) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "time>='" + from.toString() + "' AND time<='" + to.toString() + "'";
      return this;

    }

    public Select until(Instant to) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "time<='" + to.toString() + "'";
      return this;

    }

    public Select from(Instant from) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += " time>='" + from.toString() + "'";
      return this;

    }

    public List<MarketData> select() {

      List<MarketData> data = new ArrayList<>();

      Query q = new Query("SELECT * FROM marketdata WHERE " + where, keyspace);
      logger.debug("Executing select: " + q.getCommand());
      QueryResult result = influx.query(q);

      logger.debug("Select returned " + result.getResults().size());

      if (StringUtils.isEmpty(result.getError()) && result.getResults().size() > 0 && result.getResults().get(0).getSeries() != null) {

        result.getResults().get(0).getSeries().stream().map(s -> {
          return InfluxMarketDataDto.convert(s, stream);
        }).forEach(l -> {
          l.stream().forEach(m -> {
            data.add(m.toMarketData());
          });
        });
      }

      return data;
    }
  }

  public class Count {

    private String query = "";
    private String where = "";

    Count(String type) {

      if (type.equalsIgnoreCase(Bar.TYPE)) {
        query += "count(open)";
      }else if (type.equalsIgnoreCase(Quote.TYPE)) {
        query += "count(ask)";
      }else if (type.equalsIgnoreCase(Tick.TYPE)) {
        query += "count(price)";
      }

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "type='" + type.toLowerCase() + "'";
    }

    public Count bySymbol(String symbol) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "symbol='" + symbol.toLowerCase() + "'";
      return this;
    }

    public Count bySource(String stream) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "source='" + stream.toLowerCase() + "'";
      return this;
    }

    public Long count() {

      Query q = new Query("SELECT " + query + " FROM marketdata WHERE " + where, keyspace);
      logger.debug("Executing count: " + q.getCommand());
      QueryResult result = influx.query(q);

      logger.debug("Count result: " + result.toString());

      if (StringUtils.isEmpty(result.getError()) && result.getResults().size() > 0 && result.getResults().get(0).getSeries() != null) {

        Series series = result.getResults().get(0).getSeries().get(0);
        int count = series.getColumns().indexOf("count");
        return ((Double) series.getValues().get(0).get(count)).longValue();
      }

      return Long.valueOf(0);
    }
  }

  public class Delete {

    private String where = "";

    Delete() {
    }

    public Delete bySymbol(String symbol) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "symbol='" + symbol.toLowerCase() + "'";
      return this;
    }

    public Delete byType(String type) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "type='" + type.toLowerCase() + "'";
      return this;
    }


    public Delete byInterval(String interval) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "interval='" + interval + "'";
      return this;
    }

    public Delete bySource(String stream) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "source='" + stream.toLowerCase() + "'";
      return this;
    }

    public Delete between(Instant from, Instant to) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "time=> " + from.toEpochMilli() + " AND time=<" + to.toEpochMilli();
      return this;

    }

    public Delete until(Instant to) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "time=<" + to.toEpochMilli();
      return this;

    }

    public Delete from(Instant from) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += " time=> " + from.toEpochMilli();
      return this;

    }

    public void delete() {
      Query q = null;
      if (!StringUtils.isEmpty(where)) {
        q = new Query("DELETE FROM marketdata WHERE " + where, keyspace);
      } else {
        q = new Query("DROP MEASUREMENT marketdata", keyspace);
      }
      logger.debug("Executing delete: " + q.getCommand());
      QueryResult result = influx.query(q);

      logger.debug("Delete result: " + result.toString());

      if (!StringUtils.isEmpty(result.getResults().get(0).getError()) && result.getResults().get(0).getError().contains("not found")) {
        //that's fine
      } else if (result.getError() != null || result.getResults().get(0).getSeries() != null || result.getResults().get(0).getError() != null) {
        throw new RuntimeException("Delete per " + q.getCommand() + " failed");
      }
    }
  }
}




