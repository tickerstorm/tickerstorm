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
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.data.query.YahooHistoricalQuoteQuery;

@Component
public class YahooHistoricalQuoteConverter implements DataConverter {

  public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

  @Override
  public Set<String> namespaces() {
    return Sets.newHashSet(YahooHistoricalQuoteQuery.HOST);
  }

  public Candle[] convert(String line, DataQuery dq) {

    if (line.contains("Date"))
      return null;

    String[] args = line.split(",");

    String cInterval = Candle.EOD;

    if (dq.getInterval().equals("d"))
      cInterval = Candle.EOD;
    if (dq.getInterval().equals("w"))
      cInterval = Candle.WEEK_INTERVAL;

    Candle c = new Candle(dq.getSymbol(), "yahoo", LocalDate.parse(args[0], FORMATTER).atTime(0, 0).toInstant(ZoneOffset.ofHours(-7)),
        new BigDecimal(args[1]), new BigDecimal(args[1]), new BigDecimal(args[2]), new BigDecimal(args[3]), cInterval,
        new Integer(args[5]));
    c.stream = provider();
    return new Candle[] {c};
  }


  @Override
  public String provider() {
    return "Yahoo";
  }


  public Mode mode() {
    return Mode.line;
  }

}
