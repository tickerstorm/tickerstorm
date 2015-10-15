package io.tickerstorm.common.entity;

import java.time.Instant;

public interface Field<T> {

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

}
