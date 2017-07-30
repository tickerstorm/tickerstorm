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

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Field.Name;
import io.tickerstorm.common.entity.MarketData;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * Created by kkarski on 4/10/17.
 */
@Repository
public class InfluxModelDataDao {


  protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(InfluxModelDataDao.class);

  @Value("${influx.keyspace}")
  private String keyspace;

  @Autowired
  private InfluxDB influx;

  public Select newSelect(String stream) {
    return new Select(stream);
  }

  public Set<String> allHeaders(String stream) {
    return newSelect(stream).selectHeaders(stream);
  }

  public long count(String stream) {
    return newSelect(stream).count();
  }

  public List<Set<Field<?>>> findAll(String stream) {
    return newSelect(stream).select();
  }

  public void ingestMarketData(Collection<MarketData> mds) {
    mds.stream().forEach(md -> {
      ingest(md.getFields());
    });
  }

  public void ingest(Collection<Field<?>> dtos) {
    InfluxModelDataDto dto = InfluxModelDataDto.convert(dtos);
    for (Point p : dto.getPoints()) {
      influx.write(keyspace, "autogen", p);
    }
  }

  public void ingest(Field<?> f) {
    InfluxModelDataDto dto = InfluxModelDataDto.convert(f);
    for (Point p : dto.getPoints()) {
      influx.write(keyspace, "autogen", p);
    }
  }

  public void ingestMarketData(MarketData md) {
    ingest(md.getFields());
  }

  public void deleteByStream(String stream) {
    newSelect(stream).delete();
  }

  public class Select {

    private String where = "";
    private String from = InfluxModelDataDto.PREFIX;

    Select(String stream) {
      this.from += stream.toLowerCase();
    }

    public Select byStream(String stream) {
      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "stream='" + stream.toLowerCase() + "'";
      return this;
    }

    public Select byType(Class<?> type) {

      if (!StringUtils.isEmpty(where)) {
        where += " AND ";
      }

      where += "type='" + type.getSimpleName() + "'";
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

    public void delete() {

      Query q = null;
      if (!StringUtils.isEmpty(where)) {
        q = new Query("DELETE FROM \"" + from + "\" WHERE " + where, keyspace);
      } else {
        q = new Query("DROP MEASUREMENT \"" + from + "\"", keyspace);
      }

      logger.debug("Executing delete: " + q.getCommand());
      QueryResult result = influx.query(q);

      logger.debug("Delete returned " + result.toString());

      if (result.getError() == null && result.getResults().get(0).getSeries() == null && result.getResults().get(0).getError() == null) {
        //success
      } else if (!StringUtils.isEmpty(result.getResults().get(0).getError()) && result.getResults().get(0).getError().contains("not found")) {
        //that's fine
      } else {
        throw new RuntimeException("Delete per " + q.getCommand() + " failed");
      }

    }

    public Long count() {

      Query q = null;

      if (!StringUtils.isEmpty(where)) {
        q = new Query("SELECT count(*) FROM \"" + from + "\" WHERE " + where, keyspace);
      } else {
        q = new Query("SELECT count(*) FROM \"" + from + "\"", keyspace);
      }

      logger.debug("Executing count: " + q.getCommand());
      QueryResult result = influx.query(q);

      logger.debug("Count returned " + result.toString());

      if (StringUtils.isEmpty(result.getError()) && result.getResults().size() > 0 && result.getResults().get(0).getSeries() != null) {

        Optional<Object> maxCount = result.getResults().get(0).getSeries().get(0).getValues().get(0).stream().max(new Comparator<Object>() {
          @Override
          public int compare(Object o1, Object o2) {

            if (o1 instanceof Double && o2 instanceof Double) {
              return ((Double) o1).compareTo((Double) o2);
            }

            return -1;
          }
        });

        return ((Double) maxCount.orElse(0D)).longValue();
      }

      return Long.valueOf(0);
    }

    protected Set<String> selectHeaders(String stream) {

      Set<String> data = new HashSet<>();

      Query q1 = null;
      Query q2 = null;

      if (!StringUtils.isEmpty(stream)) {
        q1 = new Query("SHOW FIELD KEYS ON " + keyspace + " FROM \"" + InfluxModelDataDto.PREFIX + stream.toLowerCase() + "\"", keyspace);
        q2 = new Query("SHOW TAG KEYS ON " + keyspace + " FROM \"" + InfluxModelDataDto.PREFIX + stream.toLowerCase() + "\"", keyspace);
      }

      logger.debug("Executing select: " + q1.getCommand());
      logger.debug("Executing select: " + q2.getCommand());
      QueryResult result1 = null;
      QueryResult result2 = null;

      try {
        result1 = influx.query(q1);
        result2 = influx.query(q2);
      } catch (Exception e) {
        logger.error(q1.getCommand(), e);
        logger.error(q2.getCommand(), e);
        logger.error(e.getMessage(), e);
        throw e;
      }

      logger.debug("Select returned " + result1.toString());
      logger.debug("Select returned " + result2.toString());

      if (StringUtils.isEmpty(result1.getError()) && result1.getResults().size() > 0 && result1.getResults().get(0).getSeries() != null) {
        result1.getResults().get(0).getSeries().get(0).getValues().stream().forEach(r -> {
          data.add((String) r.get(0));
        });
      }

      if (StringUtils.isEmpty(result2.getError()) && result2.getResults().size() > 0 && result2.getResults().get(0).getSeries() != null) {
        result2.getResults().get(0).getSeries().get(0).getValues().stream().forEach(r -> {
          data.add((String) r.get(0));
        });
      }

      if (!data.isEmpty()) {
        //always there
        data.add(Name.TIMESTAMP.field());
        data.add(Name.STREAM.field());
        data.add(Name.SYMBOL.field());
      }

      return data;
    }

    public List<Set<Field<?>>> select() {

      List<Set<Field<?>>> data = new ArrayList<>();

      Query q = null;

      if (!StringUtils.isEmpty(where)) {
        q = new Query("SELECT * FROM \"" + from + "\" WHERE " + where, keyspace);
      } else {
        q = new Query("SELECT * FROM \"" + from + "\"", keyspace);
      }

      logger.debug("Executing select: " + q.getCommand());
      QueryResult result = null;

      try {
        result = influx.query(q);
      } catch (Exception e) {
        logger.error(q.getCommand(), e);
        logger.error(e.getMessage(), e);
        throw e;
      }

      logger.debug("Select returned " + result.getResults().size());

      if (StringUtils.isEmpty(result.getError()) && result.getResults().size() > 0 && result.getResults().get(0).getSeries() != null) {
        InfluxModelDataDto.convert(result.getResults().get(0).getSeries().get(0)).stream().forEach(dto -> {
          data.add(dto.asFields());
        });
      }

      return data;
    }

    public void select(int batchSize, Consumer<Set<Field<?>>> consumer) {

      Query q = null;

      if (!StringUtils.isEmpty(where)) {
        q = new Query("SELECT * FROM \"" + from + "\" WHERE " + where, keyspace);
      } else {
        q = new Query("SELECT * FROM \"" + from + "\"", keyspace);
      }

      logger.debug("Executing select: " + q.getCommand());
      QueryResult result = null;

      try {

        influx.query(q, batchSize, new Consumer<QueryResult>() {
          @Override
          public void accept(QueryResult result) {

            if (StringUtils.isEmpty(result.getError()) && result.getResults().size() > 0 && result.getResults().get(0).getSeries() != null) {
              InfluxModelDataDto.convert(result.getResults().get(0).getSeries().get(0)).stream().forEach(dto -> {
                consumer.accept(dto.asFields());
              });
            }
          }

        });


      } catch (Exception e) {
        logger.error(q.getCommand(), e);
        logger.error(e.getMessage(), e);
        throw e;
      }

    }
  }
}
