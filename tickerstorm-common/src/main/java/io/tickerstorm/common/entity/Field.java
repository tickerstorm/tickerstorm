package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import io.tickerstorm.common.command.Marker;
import io.tickerstorm.common.data.converter.Util;

/**
 *
 * @author kkarski
 *
 * @param <T>
 */
public interface Field<T> extends Serializable, Comparable<Field<T>> {

  public static Comparator<Instant> SORT_BY_INSTANTS = new Comparator<Instant>() {

    @Override
    public int compare(Instant o1, Instant o2) {
      return o2.compareTo(o1);
    }

  };

  public static Comparator<Field<?>> SORT_REVERSE_FIELDS = new Comparator<Field<?>>() {

    @Override
    public int compare(Field<?> o1, Field<?> o2) {
      return o1.getTimestamp().compareTo(o2.getTimestamp());
    }

  };

  public static Comparator<Instant> SORT_REVERSE_INSTANTS = new Comparator<Instant>() {

    @Override
    public int compare(Instant o1, Instant o2) {
      return o1.compareTo(o2);
    }

  };

  public enum Name {

    SYMBOL("symbol", String.class), TIMESTAMP("timestamp", Instant.class), SOURCE("source", String.class), STREAM("stream",
        String.class), OPEN("open", BigDecimal.class), CLOSE("close", BigDecimal.class), HIGH("high", BigDecimal.class), LOW("low",
            BigDecimal.class), BID("bid", BigDecimal.class), ASK("ask", BigDecimal.class), BID_SIZE("bidSize",
                BigDecimal.class), ASK_SIZE("askSize", BigDecimal.class), VOLUME("volume", Integer.class), PRICE("price",
                    BigDecimal.class), QUANTITY("quantity", Integer.class), INTERVAL("interval", String.class), MARKETDATA("marketdata",
                        MarketData.class, 0), CANDEL("candel", Bar.class), QUOTE("quote", Quote.class), TICK("tick", Tick.class), AVE("ave",
                            BigDecimal.class), SMA("ma", BigDecimal.class), MEDIAN("median", BigDecimal.class), STD("std",
                                BigDecimal.class), NOW("now", Instant.class), MARKER("marker", Marker.class), FEATURES("features",
                                    Collection.class), DISCRETE_FIELDS("discrete_fields", Collection.class,
                                        2), CONTINOUS_FIELDS("continous_fields", Collection.class, 3), TEMPORAL_FIELDS("temporal_fields",
                                            Collection.class, 4), CATEGORICAL_FIELDS("categorical_fields", Collection.class, 1), MIN("min",
                                                Collection.class), MAX("max", Collection.class), PCT_CHANGE("pct_change",
                                                    Collection.class), ABS_CHANGE("abs_change", Collection.class);

    private Class<?> type;
    private String field;
    private int index;

    private Name(String name, Class<?> clazz) {
      this.type = clazz;
      this.field = name;
      this.index = -1;
    }

    private Name(String name, Class<?> clazz, int index) {
      this.type = clazz;
      this.field = name;
      this.index = index;
    }

    public String field() {
      return field;
    }

    public int index() {
      return index;
    }

    @Override
    public String toString() {
      return field;
    }

    public Class<?> fieldType() {
      return type;
    }
  }

  /**
   * 
   * @param value
   * @return
   */
  static Field<?> deserialize(String value) {

    Class<?> clazz = parseType(value);
    Object vals = parseValue(value, clazz);
    String field = parseField(value);
    String eventId = parseEventId(value);
    return new BaseField(eventId, field, vals, clazz);

  }

  static Field<?> findField(String name, Set<Field<?>> fields) {

    for (Field<?> f : fields) {
      if (name.equalsIgnoreCase(f.getName()))
        return f;
    }

    return null;

  }

  /**
   * Find the field's event id in the input string. This method is not compaible with the market
   * data event's eventId. Must be a field's event id
   * 
   * @param value
   * @return
   */
  static String parseEventId(String value) {
    String[] vals = value.split(":");
    return vals[1];
  }

  /**
   * Find the field's timestamp value from a serialized field string. This method is not compatible
   * with the market data event's eventId. Must be a field's event id
   * 
   * @param value
   * @return
   */
  static Instant parseTimestamp(String value) {
    return MarketData.parseTimestamp(parseEventId(value));
  }


  static String parseField(String value) {
    String[] vals = value.split("=");
    String[] fields = vals[0].split(":");
    return fields[2];
  }

  static String parseStream(String value) {
    return MarketData.parseStream(parseEventId(value));
  }

  static String parseSymbol(String value) {
    return MarketData.parseSymbol(parseEventId(value));
  }

  static Class<?> parseType(String value) {
    String[] vals = value.split(":");

    if (String.class.getName().equalsIgnoreCase(vals[0])) {
      return String.class;
    } else if (Integer.class.getName().equalsIgnoreCase(vals[0])) {
      return Integer.class;
    } else if (BigDecimal.class.getName().equalsIgnoreCase(vals[0])) {
      return BigDecimal.class;
    } else if (Instant.class.getName().equalsIgnoreCase(vals[0])) {
      return Instant.class;
    }

    return String.class;
  }

  static <T> T parseValue(String value, Class<T> clazz) {
    String[] vals = value.split("=");

    if (vals[1].equalsIgnoreCase("null"))
      return null;

    return Util.convert(vals[1], clazz);
  }

  static Map<String, Field<?>> toMap(Set<Field<?>> fields) {

    Map<String, Field<?>> map = new HashMap<>(fields.size());

    for (Field<?> f : fields) {
      map.put(f.getName(), f);
    }

    return map;
  }

  /**
   * Should take form
   * 
   * @return
   */
  public String getEventId();

  public Class<?> getFieldType();

  public String getName();

  default Instant getTimestamp() {
    return MarketData.parseTimestamp(getEventId());
  }

  default String getInterval() {
    return MarketData.parseInterval(getEventId());
  }

  default String getStream() {
    return MarketData.parseStream(getEventId());
  }

  default String getSymbol() {
    return MarketData.parseSymbol(getEventId());
  }

  public T getValue();

  default boolean isNull() {

    if (getValue() == null)
      return true;

    if (getFieldType() != null && getFieldType().equals(String.class))
      return StringUtils.isEmpty((String) getValue());

    return false;

  }

  /**
   * 
   * Serialized: type:eventId:fieldName=value
   * 
   * type : 0, eventId: 1, name: 2
   * 
   * @return
   */
  default String serialize() {
    StringBuffer buff = new StringBuffer(getFieldType().getName()).append(":").append(getEventId()).append(":").append(getName())
        .append("=").append(getValue());
    return buff.toString();
  }

}
