package io.tickerstorm.common.data.feed;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;

import io.tickerstorm.common.entity.Candle;

@SuppressWarnings("serial")
public class HistoricalFeedQuery implements Serializable {

  public ZoneOffset zone = ZoneOffset.UTC;
  public Set<String> symbols = new HashSet<>();
  public LocalDateTime from = LocalDateTime.now().minusYears(1);
  public LocalDateTime until = LocalDateTime.now();
  public String source = "google";
  public Set<String> periods = Sets.newHashSet(Candle.MIN_1_INTERVAL);
  public final String id = UUID.randomUUID().toString();

  public HistoricalFeedQuery(LocalDateTime from, LocalDateTime until, String... symbols) {
    this.symbols.addAll(Sets.newHashSet(symbols));
    this.from = from;
    this.until = until;
  }

  public HistoricalFeedQuery(String... symbols) {
    this.symbols.addAll(Sets.newHashSet(symbols));
  }

  public HistoricalFeedQuery() {

  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("symbol", symbols).add("from", from).add("until", until).toString();
  }

}
