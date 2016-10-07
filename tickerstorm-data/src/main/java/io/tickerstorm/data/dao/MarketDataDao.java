package io.tickerstorm.data.dao;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
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

@Repository
public class MarketDataDao {

  private final static Logger logger = LoggerFactory.getLogger(MarketDataDao.class);

  @Value("${cassandra.keyspace}")
  protected String keyspace;

  @Autowired
  private CassandraOperations cassandra;

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

    BigInteger until = new BigInteger(ModelDataDto.dateFormatter.format(Instant.now()));

    String select = "SELECT token(stream, date), stream, date, timestamp, fields FROM " + keyspace + ".modeldata "
        + "WHERE token(stream, date) <= token('" + stream.toLowerCase() + "', " + until + ");";

    logger.debug("Executing " + select);

    List<MarketDataDto> dtos = cassandra.selectAll(MarketDataDto.class);
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
