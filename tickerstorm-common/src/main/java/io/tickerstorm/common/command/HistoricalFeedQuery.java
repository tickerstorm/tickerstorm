package io.tickerstorm.common.command;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.Sets;

import io.tickerstorm.common.entity.Bar;

@SuppressWarnings("serial")
public class HistoricalFeedQuery extends Command implements DataFeedQuery {

  public ZoneOffset zone = ZoneOffset.UTC;
  public Set<String> symbols = new HashSet<>();
  public LocalDateTime from = LocalDateTime.now(ZoneId.of("UTC")).minusYears(1);
  public LocalDateTime until = LocalDateTime.now(ZoneId.of("UTC"));
  public Set<String> periods = Sets.newHashSet(Bar.MIN_1_INTERVAL);

  // Stream the data is being sourced from
  public String source;

  public HistoricalFeedQuery(String stream, String source) {
    super(stream, "query.marketdata");
    this.source = source;
    this.markers.add(Markers.MARKET_DATA.toString());
    this.markers.add(Markers.QUERY.toString());
  }

  public HistoricalFeedQuery(String stream, String source, String... symbols) {
    super(stream, "query.marketdata");
    this.symbols.addAll(Sets.newHashSet(symbols));
    this.source = source;
    this.markers.add(Markers.MARKET_DATA.toString());
    this.markers.add(Markers.QUERY.toString());
  }

  public Predicate<Notification> isDone() {
    return p -> p.is(Markers.MARKET_DATA.toString()) && p.is(Markers.QUERY.toString()) && p.is(Markers.END.toString())
        && p.getStream().equalsIgnoreCase(getStream()) && p.id.equals(id);
  }

  public Predicate<Notification> isFailed() {
    return p -> p.is(Markers.MARKET_DATA.toString()) && p.is(Markers.QUERY.toString()) && p.is(Markers.FAILED.toString())
        && p.getStream().equalsIgnoreCase(getStream()) && p.id.equals(id);
  }

  public Predicate<Notification> isStarted() {
    return p -> p.is(Markers.MARKET_DATA.toString()) && p.is(Markers.QUERY.toString()) && p.is(Markers.START.toString())
        && p.getStream().equalsIgnoreCase(getStream()) && p.id.equals(id);
  }
  
   @Override
  public boolean isValid() {
    return (super.validate() && !symbols.isEmpty() && from != null && until != null && from.isBefore(until) && !periods.isEmpty());
  }
}
