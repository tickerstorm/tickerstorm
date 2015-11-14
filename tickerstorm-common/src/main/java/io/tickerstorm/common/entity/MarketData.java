package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public interface MarketData extends Event, Serializable {

  public String getSymbol();

  public Set<Field<?>> getFields();
  
  public static boolean validate(Field<?>[] fields) {

    String symbol = fields[0].getSymbol();
    String source = fields[0].getSource();
    Instant timestamp = fields[0].getTimestamp();
    String inteval = fields[0].getInterval();

    for (Field<?> f : fields) {

      if (!f.getSymbol().equalsIgnoreCase(symbol))
        throw new IllegalArgumentException("All fields must belong to the same symbol");

      if (!f.getSource().equalsIgnoreCase(source))
        throw new IllegalArgumentException("All fields must belong to the same source");

      if (!f.getTimestamp().equals(timestamp))
        throw new IllegalArgumentException("All fields must belong to the same timestamp");

      if (!f.getInterval().equals(inteval))
        throw new IllegalArgumentException("All fields must belong to the same interval");

    }

    return true;

  }

  public static String getMarketDataType(Field<?>[] fields) {

    String inteval = fields[0].getInterval();

    if (!StringUtils.isEmpty(inteval))
      return Candle.TYPE;


    for (Field<?> f : fields) {

      if (f.getName().equalsIgnoreCase(Field.ASK))
        return Quote.TYPE;

      if (f.getName().equalsIgnoreCase(Field.PRICE))
        return Tick.TYPE;
    }

    throw new IllegalArgumentException("Unknown market data type");

  }

  public static MarketData build(Field<?>[] fields) {

    boolean validated = validate(fields);

    if (validated) {
      switch (getMarketDataType(fields)) {
        case Candle.TYPE:
          return new Candle(fields);

        case Quote.TYPE:
          return new Quote(fields);

        case Tick.TYPE:
          return new Tick(fields);
        
        default:
          break;
      }
    }

    throw new IllegalArgumentException("Unknown market data type");

  }

}
