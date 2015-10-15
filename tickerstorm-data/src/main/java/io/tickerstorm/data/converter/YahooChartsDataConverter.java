package io.tickerstorm.data.converter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.TimeZone;

import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.tickerstorm.common.data.converter.DataConverter;
import io.tickerstorm.common.data.converter.Mode;
import io.tickerstorm.common.data.query.DataQuery;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.data.query.YahooChartsDataQuery;

@Component
public class YahooChartsDataConverter implements DataConverter {

  @Override
  public Set<String> namespaces() {
    return Sets.newHashSet(YahooChartsDataQuery.HOST);
  }

  public Mode mode() {
    return Mode.line;
  }

  public String provider() {
    return "Yahoo";
  }

  public Candle[] convert(String line, DataQuery query) {

    TimeZone timezone = null;

    if (line.contains("timezone:")) {
      timezone = TimeZone.getTimeZone(line.split(":")[1]);
      return null;
    }

    if (line.contains(":")) {
      return null;
    }

    String[] args = line.split(",");

    Candle c = new Candle(query.getSymbol(), "yahoo", Instant.ofEpochSecond(Long.valueOf(args[0])), new BigDecimal(args[4]),
        new BigDecimal(args[1]), new BigDecimal(args[2]), new BigDecimal(args[3]), query.getInterval(), new Integer(args[5]));

    return new Candle[] {c};
  }

}
