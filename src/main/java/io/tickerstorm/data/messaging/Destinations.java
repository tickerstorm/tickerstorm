package io.tickerstorm.data.messaging;

public interface Destinations {

  public static final String TOPIC_REALTIME_MARKETDATA = "topic://topic.realtime.marketdata";
  public static final String TOPIC_HISTORICAL_MARKETDATA = "topic://topic.historical.marketdata";
  public static final String QUEUE_QUERY = "queue://queue.query";

}
