package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface MarketData extends Event, Stream, Serializable {

  public String getSymbol();

  default Map<String, Field<?>> getFieldsAsMap() {
    return Field.toMap(getFields());
  }

  public Set<Field<?>> getFields();

  public static String getMarketDataType(Set<Field<?>> fields) {

    Field<String> interval = (Field<String>) Field.findField(Field.Name.INTERVAL.field(), fields);

    if (interval != null)
      return Candle.TYPE;

    if (Field.findField(Field.Name.ASK.field(), fields) != null)
      return Quote.TYPE;

    if (Field.findField(Field.Name.PRICE.field(), fields) != null)
      return Tick.TYPE;

    throw new IllegalArgumentException("Unknown market data type");

  }

  public static MarketData build(Set<Field<?>> fields) {



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


    throw new IllegalArgumentException("Unknown market data type");

  }

}
