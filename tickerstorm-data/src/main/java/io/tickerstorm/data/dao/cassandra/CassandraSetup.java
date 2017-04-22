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

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;

@Repository
public class CassandraSetup {

  public final static Logger logger = LoggerFactory.getLogger(CassandraSetup.class);

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @Autowired
  private CassandraOperations session;

  @PostConstruct
  public void init() {

    CreateKeyspaceSpecification spec = CreateKeyspaceSpecification.createKeyspace(keyspace).ifNotExists().withSimpleReplication(3);
    ResultSet set = session.execute(spec);

    logger.info("Cassandra keyspace: " + "USE " + keyspace);
    session.execute("USE " + keyspace);

    CreateTableSpecification tableSpec = CreateTableSpecification.createTable("marketdata").partitionKeyColumn("symbol", DataType.text())
        .partitionKeyColumn("date", DataType.varint()).clusteredKeyColumn("type", DataType.text(), Ordering.ASCENDING)
        .clusteredKeyColumn("source", DataType.text(), Ordering.ASCENDING)
        .clusteredKeyColumn("interval", DataType.text(), Ordering.ASCENDING)
        .clusteredKeyColumn("timestamp", DataType.timestamp(), Ordering.ASCENDING)
        .clusteredKeyColumn("hour", DataType.cint(), Ordering.ASCENDING).clusteredKeyColumn("min", DataType.cint(), Ordering.ASCENDING)
        .column("ask", DataType.decimal()).column("askSize", DataType.decimal()).column("bid", DataType.decimal())
        .column("bidSize", DataType.decimal()).column("close", DataType.decimal()).column("high", DataType.decimal())
        .column("low", DataType.decimal()).column("open", DataType.decimal()).column("price", DataType.decimal())
        .column("properties", DataType.map(DataType.text(), DataType.text())).column("volume", DataType.decimal())
        .column("quantity", DataType.decimal()).ifNotExists();

    CreateIndexSpecification indexSpec =
        CreateIndexSpecification.createIndex("marketdata_source_index").columnName("source").tableName("marketdata").ifNotExists();

    CreateTableSpecification modelSpec = CreateTableSpecification.createTable("modeldata").partitionKeyColumn("stream", DataType.text())
        .partitionKeyColumn("date", DataType.varint()).clusteredKeyColumn("timestamp", DataType.timestamp(), Ordering.DESCENDING)
        .column("fields", DataType.set(DataType.text())).ifNotExists();

    CreateIndexSpecification indexSpec2 =
        CreateIndexSpecification.createIndex("model_stream_index").columnName("stream").tableName("modeldata").ifNotExists();

    set = session.execute(tableSpec);
    set = session.execute(indexSpec);
    set = session.execute(modelSpec);
    set = session.execute(indexSpec2);
  }

}
