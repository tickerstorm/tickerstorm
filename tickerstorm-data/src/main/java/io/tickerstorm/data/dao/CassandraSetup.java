package io.tickerstorm.data.dao;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cassandra.core.Ordering;
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
        .partitionKeyColumn("date", DataType.text()).clusteredKeyColumn("type", DataType.text(), Ordering.ASCENDING)
        .clusteredKeyColumn("source", DataType.text(), Ordering.ASCENDING)
        .clusteredKeyColumn("interval", DataType.text(), Ordering.ASCENDING)
        .clusteredKeyColumn("timestamp", DataType.timestamp(), Ordering.ASCENDING)
        .clusteredKeyColumn("hour", DataType.cint(), Ordering.ASCENDING).clusteredKeyColumn("min", DataType.cint(), Ordering.ASCENDING)
        .column("ask", DataType.decimal()).column("askSize", DataType.decimal()).column("bid", DataType.decimal())
        .column("bidSize", DataType.decimal()).column("close", DataType.decimal()).column("high", DataType.decimal())
        .column("low", DataType.decimal()).column("open", DataType.decimal()).column("price", DataType.decimal())
        .column("properties", DataType.map(DataType.text(), DataType.text())).column("volume", DataType.decimal())
        .column("quantity", DataType.decimal()).ifNotExists();
    
    CreateTableSpecification modelSpec = CreateTableSpecification.createTable("modeldata").partitionKeyColumn("modelname", DataType.text())
        .partitionKeyColumn("date", DataType.text()).clusteredKeyColumn("type", DataType.text(), Ordering.DESCENDING)
        .clusteredKeyColumn("timestamp", DataType.timestamp(), Ordering.DESCENDING)
        .column("fields", DataType.map(DataType.text(), DataType.text())).ifNotExists();

    set = session.execute(tableSpec);
    set = session.execute(modelSpec);
  }

}
