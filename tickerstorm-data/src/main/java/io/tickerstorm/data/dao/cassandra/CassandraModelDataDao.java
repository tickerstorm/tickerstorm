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

package io.tickerstorm.data.dao.cassandra;

import io.tickerstorm.data.dao.ModelDataDao;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Lists;

@Repository
public class CassandraModelDataDao implements ModelDataDao {

  private final static Logger logger = LoggerFactory.getLogger(CassandraModelDataDao.class);

  public static final java.time.format.DateTimeFormatter dateFormat2 =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));

  @Value("${cassandra.keyspace}")
  protected String keyspace;

  @Autowired
  private CassandraOperations cassandra;

  @Override
  public long count() {
    return cassandra.count(CassandraModelDataDto.class);
  }

  @Override
  public long count(String stream) {
    Select select = QueryBuilder.select("stream", "date").from("modeldata");
    select.where(QueryBuilder.eq("stream", stream.toLowerCase()));

    logger.debug("Executing " + select.toString());

    ResultSet result = cassandra.getSession().execute(select.toString());
    return result.all().size();
  }

  @Override
  public Stream<CassandraModelDataDto> findAll(String stream) {

    Select select = QueryBuilder.select().from("modeldata");
    select.where(QueryBuilder.eq("stream", stream.toLowerCase())).and(QueryBuilder.lte("timestamp", Date.from(Instant.now())))
        .and(QueryBuilder.in("date", findAllDatesByStream(stream)));
    logger.debug("Executing " + select);

    List<CassandraModelDataDto> dtos = cassandra.select(select, CassandraModelDataDto.class);
    return dtos.stream();
  }

  @Override
  public void ingest(Collection<CassandraModelDataDto> dtos) {

    List<List<?>> rows = new ArrayList<>();
    dtos.stream().forEach(r -> {
      rows.add(Lists.newArrayList(r.fields, r.primarykey.stream.toLowerCase(), r.primarykey.date, r.primarykey.timestamp));
    });

    cassandra.ingest("UPDATE " + keyspace + ".modeldata SET fields = fields + ? WHERE stream = ? AND date = ? AND timestamp = ?;", rows);

  }

  @Override
  public List<CassandraModelDataDto> findByStreamAndTimestampIsBetween(String stream, Instant from, Instant until) {

    Select select = QueryBuilder.select().from(keyspace, "modeldata");
    select.where(QueryBuilder.eq("stream", stream.toLowerCase())).and(QueryBuilder.in("date", dates(from, until)))
        .and(QueryBuilder.gte("timestamp", Date.from(from))).and(QueryBuilder.lte("timestamp", Date.from(until)));

    logger.debug("Executing " + select);
    long startTimer = System.currentTimeMillis();
    List<CassandraModelDataDto> s = cassandra.select(select, CassandraModelDataDto.class);
    logger.info("Query took " + (System.currentTimeMillis() - startTimer) + "ms to fetch " + s.size() + " results.");
    return s;
  }

  private List<BigInteger> dates(Instant from, Instant until) {

    Instant date = from;
    List<BigInteger> dates = new ArrayList<>();
    dates.add(new BigInteger(dateFormat2.format(date)));

    while (date.compareTo(until) < 0) {
      date = date.plus(1, ChronoUnit.DAYS);
      dates.add(new BigInteger(dateFormat2.format(date)));
    }

    return dates;
  }

  private List<BigInteger> findAllDatesByStream(String stream) {
    Select select = QueryBuilder.select("stream", "date").from("modeldata");
    select.where(QueryBuilder.eq("stream", stream.toLowerCase()));
    ResultSet result = cassandra.getSession().execute(select.toString());
    List<Row> rows = result.all();

    List<BigInteger> dates = rows.stream().map(r -> {
      return r.getVarint("date");
    }).distinct().collect(Collectors.toList());

    return dates;
  }

  @Override
  public void deleteByStream(String stream) {

    List<BigInteger> dates = findAllDatesByStream(stream);
    Delete delete = QueryBuilder.delete().from("modeldata");
    delete.where(QueryBuilder.eq("stream", stream.toLowerCase())).and(QueryBuilder.in("date", dates));
    logger.debug("Executing " + delete);
    cassandra.execute(delete);

  }

  @Override
  public Stream<CassandraModelDataDto> streamByStreamAndDateIn(String stream, Set<BigInteger> dates) {

    Select select = QueryBuilder.select().from("modeldata");
    select.where(QueryBuilder.eq("stream", stream.toLowerCase())).and(QueryBuilder.in("date", dates));

    logger.debug("Executing " + select);

    List<CassandraModelDataDto> s = cassandra.select(select, CassandraModelDataDto.class);
    return s.stream();

  }

}
