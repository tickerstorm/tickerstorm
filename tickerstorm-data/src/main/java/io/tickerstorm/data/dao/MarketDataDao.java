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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

@Repository
public class MarketDataDao {

  private final static Logger logger = LoggerFactory.getLogger(MarketDataDao.class);

  @Value("${cassandra.keyspace}")
  protected String keyspace;

  @Autowired
  private CassandraOperations cassandra;

  public void ingest(Collection<MarketDataDto> dtos) {

    List<List<?>> rows = new ArrayList<>();

    String insert = "INSERT INTO " + keyspace + ".marketdata "
        + "(symbol, date, type, source, interval, timestamp, hour, min, ask, asksize, bid, bidsize, close, high, low, open, price, properties, quantity, volume) "
        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

    dtos.stream().forEach(r -> {
      rows.add(Lists.newArrayList(r.primarykey.symbol, r.primarykey.date, r.primarykey.type,
          r.primarykey.stream, r.primarykey.interval,
          r.primarykey.timestamp, r.primarykey.hour, r.primarykey.min, r.ask, r.askSize, r.bid,
          r.bidSize, r.close, r.high, r.low, r.open,
          r.price, r.properties, r.quantity, r.volume));
    });

    cassandra.ingest(insert, rows);

  }

  public Map<String, Integer> countEntries(Collection<MarketDataDto> data) {
    Map<String, Integer> streamCounts = Maps.newHashMap();
    for (MarketDataDto dto : data) {
      Integer count = streamCounts.putIfAbsent(dto.primarykey.stream, 1);

      if (count != null) {
        streamCounts.replace(dto.primarykey.stream, count++);
      }
    }
    return streamCounts;
  }

  public long count() {
    return cassandra.count(MarketDataDto.class);
  }

  public long count(String stream) {
    Select select = QueryBuilder.select("symbol", "date").distinct().from("marketdata");
    select.where(QueryBuilder.eq("source", stream.toLowerCase()));

    logger.debug("Select " + select.toString());

    ResultSet result = cassandra.getSession().execute(select.toString());
    return result.all().size();
  }

  public Stream<MarketDataDto> findAll(String stream) {

    Select select = QueryBuilder.select().from("marketdata");
    select.where(QueryBuilder.eq("source", stream.toLowerCase()));

    logger.debug("Select " + select.toString());

    List<MarketDataDto> dtos = cassandra.select(select, MarketDataDto.class);
    return dtos.stream();
  }

  public void deleteByStream(String stream) {

    Select select = QueryBuilder.select("symbol", "date").distinct().from("marketdata");
    select.where(QueryBuilder.eq("source", stream.toLowerCase()));

    logger.debug("Select " + select.toString());

    ResultSet result = cassandra.getSession().execute(select.toString());
    List<Row> rows = result.all();

    List<String> symbols = rows.stream().map(r -> {
      return r.getString("symbol");
    }).distinct().collect(Collectors.toList());

    List<BigInteger> dates = rows.stream().map(r -> {
      return r.getVarint("date");
    }).distinct().collect(Collectors.toList());

    Delete delete = QueryBuilder.delete().from("marketdata");
    delete.where(QueryBuilder.in("symbol", symbols)).and(QueryBuilder.in("date", dates));
    logger.debug("Delete " + delete.toString());
    cassandra.execute(delete);

  }

}
