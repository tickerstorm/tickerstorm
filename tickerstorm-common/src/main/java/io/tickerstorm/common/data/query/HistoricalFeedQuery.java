package io.tickerstorm.common.data.query;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;

import io.tickerstorm.common.entity.Candle;

@SuppressWarnings("serial")
public class HistoricalFeedQuery implements DataFeedQuery {

  public ZoneOffset zone = ZoneOffset.UTC;
  public Set<String> symbols = new HashSet<>();
  public LocalDateTime from = LocalDateTime.now(ZoneId.of("UTC")).minusYears(1);
  public LocalDateTime until = LocalDateTime.now(ZoneId.of("UTC"));
  public final String id = UUID.randomUUID().toString();
  public Set<String> periods = Sets.newHashSet(Candle.MIN_1_INTERVAL);
  
  //Stream the data will be streamed as
  public String stream;
  
  //Stream the data is being sourced from
  public String source;

  public HistoricalFeedQuery(String stream, String source) {
    this.stream = stream;
    this.source = source;
  }

  public HistoricalFeedQuery(String stream, String source, String... symbols) {
    this.symbols.addAll(Sets.newHashSet(symbols));
    this.stream = stream;
    this.source = source;
  }

  public String getStream() {
    return stream;
  }

  @Override
  public String id() {
    return id;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("symbol", symbols).add("from", from).add("until", until).toString();
  }

}
