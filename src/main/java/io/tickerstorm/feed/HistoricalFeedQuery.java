package io.tickerstorm.feed;

import io.tickerstorm.entity.Candle;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class HistoricalFeedQuery implements Serializable {

  public DateTimeZone zone = DateTimeZone.forID("EST");
  public Set<String> symbols = new HashSet<>();
  public Interval interval = new Interval(new DateTime().minusYears(1).withZone(zone), new DateTime().withZone(zone));
  public String source = "google";
  public Set<String> periods = Sets.newHashSet(Candle.MIN_1_INTERVAL);

  public HistoricalFeedQuery(Interval interval, String... symbols) {
    this.symbols.addAll(Sets.newHashSet(symbols));
    this.interval = interval;
  }

  public HistoricalFeedQuery(String... symbols) {
    this.symbols.addAll(Sets.newHashSet(symbols));
  }

  public HistoricalFeedQuery() {

  }

}
