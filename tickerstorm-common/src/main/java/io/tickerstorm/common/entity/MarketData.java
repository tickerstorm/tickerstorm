package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public interface MarketData extends Event, Serializable, Comparable<MarketData> {

  public static final Comparator<MarketData> SORT_BY_TIMESTAMP = new Comparator<MarketData>() {

    @Override
    public int compare(MarketData o1, MarketData o2) {
      return o2.getTimestamp().compareTo(o1.getTimestamp());
    }

  };

  public static final Comparator<MarketData> SORT_REVERSE_TIMESTAMP = new Comparator<MarketData>() {

    @Override
    public int compare(MarketData o1, MarketData o2) {
      return o1.getTimestamp().compareTo(o2.getTimestamp());
    }

  };

  public static MarketData build(Set<Field<?>> fields) {

    switch (getMarketDataType(fields)) {
      case Bar.TYPE:
        return new Bar(fields);

      case Quote.TYPE:
        return new Quote(fields);

      case Tick.TYPE:
        return new Tick(fields);

      default:
        break;
    }

    throw new IllegalArgumentException("Unknown market data type");

  }

  public static String getMarketDataType(Set<Field<?>> fields) {

    Field<String> interval = (Field<String>) Field.findField(Field.Name.INTERVAL.field(), fields);

    if (interval != null)
      return Bar.TYPE;

    if (Field.findField(Field.Name.ASK.field(), fields) != null)
      return Quote.TYPE;

    if (Field.findField(Field.Name.PRICE.field(), fields) != null)
      return Tick.TYPE;

    throw new IllegalArgumentException("Unknown market data type");

  }

  public static String[] parseEventId(String eventId) {
    String[] parts = eventId.split("\\|");
    return parts;
  }

  public static String parseStream(String eventId) {
    return MarketData.parseEventId(eventId)[0];
  }

  public static String parseSymbol(String eventId) {
    return MarketData.parseEventId(eventId)[1];
  }

  public static Instant parseTimestamp(String eventId) {
    String part = MarketData.parseEventId(eventId)[2];
    return Instant.ofEpochMilli(Long.valueOf(part));
  }

  /**
   * Format: stream|symbol|timestamp
   * 
   * @return
   */
  default String getEventId() {
    return new StringBuffer(getStream()).append("|").append(getSymbol()).append("|")
        .append(getTimestamp().toEpochMilli()).toString();
  }

  public Set<Field<?>> getFields();

  default Map<String, Field<?>> getFieldsAsMap() {
    return Field.toMap(getFields());
  }

  public String getStream();

  public String getSymbol();

  public Instant getTimestamp();

}
