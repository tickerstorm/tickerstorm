package io.tickerstorm.common.data.eventbus;

public interface Destinations {

  public static final String TOPIC_REALTIME_MARKETDATA = "topic.marketdata.realtime";
  public static final String TOPIC_COMMANDS = "topic.commands";
  public static final String TOPIC_NOTIFICATIONS = "topic.notifications";
  public static final String QUEUE_QUERY = "queue.query";
  public static final String QUEUE_MODEL_DATA = "queue.modeldata";
  public static final String QUEUE_RETRO_MODEL_DATA = "queue.retromodeldata";


}
