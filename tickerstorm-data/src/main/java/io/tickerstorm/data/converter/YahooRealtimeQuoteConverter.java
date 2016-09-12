package io.tickerstorm.data.converter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.tickerstorm.common.data.converter.DataConverter;
import io.tickerstorm.common.data.converter.Mode;
import io.tickerstorm.common.data.query.DataQuery;
import io.tickerstorm.common.entity.Quote;
import io.tickerstorm.data.query.YahooRealtimeQuoteQuery;

@Component
public class YahooRealtimeQuoteConverter implements DataConverter {

  @Override
  public Set<String> namespaces() {
    return Sets.newHashSet(YahooRealtimeQuoteQuery.HOST);
  }

  public Quote[] convert(String line, DataQuery dq) {
    String[] data = line.split(",");

    Quote c = new Quote(dq.getSymbol(), provider(), Instant.now());
    
    if (!data[0].equalsIgnoreCase("N/A"))
      c.ask = new BigDecimal(data[0]);
    else if (!data[1].equalsIgnoreCase("N/A"))
      c.ask = new BigDecimal(data[1]);
    else
      return null;

    c.askSize = new Integer(data[2]);

    if (!data[3].equalsIgnoreCase("N/A"))
      c.bid = new BigDecimal(data[3]);
    else if (!data[4].equalsIgnoreCase("N/A"))
      c.bid = new BigDecimal(data[4]);
    else
      return null;

    c.bidSize = new Integer(data[5]);
    c.stream = provider();

    return new Quote[] {c};
  }

  @Override
  public String provider() {
    return "Yahoo";
  }

  @Override
  public Mode mode() {
    return Mode.line;
  }

}
