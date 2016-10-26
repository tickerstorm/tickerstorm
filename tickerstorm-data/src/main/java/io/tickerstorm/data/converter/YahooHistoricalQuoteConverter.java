package io.tickerstorm.data.converter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.tickerstorm.common.data.converter.DataConverter;
import io.tickerstorm.common.data.converter.DataQuery;
import io.tickerstorm.common.data.converter.Mode;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.data.query.YahooHistoricalQuoteQuery;

@Component
public class YahooHistoricalQuoteConverter implements DataConverter {

  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

  @Override
  public Set<String> namespaces() {
    return Sets.newHashSet(YahooHistoricalQuoteQuery.HOST);
  }

  public Bar[] convert(String line, DataQuery dq) {

    if (line.contains("Date"))
      return null;

    String[] args = line.split(",");

    String cInterval = Bar.EOD;

    if (dq.getInterval().equals("d"))
      cInterval = Bar.EOD;
    if (dq.getInterval().equals("w"))
      cInterval = Bar.WEEK_INTERVAL;

    Bar c = new Bar(dq.getSymbol(), "yahoo", LocalDate.parse(args[0], FORMATTER).atTime(0, 0).toInstant(ZoneOffset.ofHours(-7)),
        new BigDecimal(args[1]), new BigDecimal(args[1]), new BigDecimal(args[2]), new BigDecimal(args[3]), cInterval,
        new Integer(args[5]));
    c.stream = provider();
    return new Bar[] {c};
  }


  @Override
  public String provider() {
    return "Yahoo";
  }


  public Mode mode() {
    return Mode.line;
  }

}
