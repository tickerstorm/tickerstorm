package io.tickerstorm.common.entity;

import java.io.Serializable;
import java.time.Instant;

public interface Field<T> extends Serializable {

  public static final String SYMBOL = "symbol";
  public static final String TIMESTAMP = "timestamp";
  public static final String SOURCE = "source";
  public static final String OPEN = "open";
  public static final String CLOSE = "close";
  public static final String HIGH = "high";
  public static final String LOW = "low";
  public static final String BID = "bid";
  public static final String ASK = "ask";
  public static final String BID_SIZE = "bidSize";
  public static final String ASK_SIZE = "askSize";
  public static final String VOLUME = "volume";
  public static final String PRICE = "price";
  public static final String QUANTITY = "quantity";

  public String getSymbol();

  public Instant getTimestamp();

  public String getName();

  public String getSource();

  public String getInterval();

  public String getFieldType();

  public T getValue();

  /**
   * 
   * Serialized: type_source_interval_name_symbol_timestamp:value
   * 
   * type : 0 source : 1 interval : 2 name : 3 symbol: 4 timestamp: 5
   * 
   * @return
   */
  default String serialize() {
    StringBuffer buff = new StringBuffer(getFieldType()).append("_").append(getSource()).append("_").append(getInterval()).append("_")
        .append(getName()).append("_").append(getSymbol()).append("_").append(getTimestamp()).append("=").append(getValue());
    return buff.toString();

  }

  static Field<?> deserialize(String value) {

    if (value.startsWith(ContinousField.TYPE))
      return ContinousField.deserialize(value);
    else if (value.startsWith(DiscreteField.TYPE))
      return DiscreteField.deserialize(value);
    else if (value.startsWith(EmptyField.TYPE))
      return EmptyField.deserialize(value);
    else if (value.startsWith(CategoricalField.TYPE))
      return CategoricalField.deserialize(value);
    else
      throw new IllegalArgumentException();

  }

}
