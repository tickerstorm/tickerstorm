package io.tickerstorm.common.data.eventbus;

public interface Destinations {

  public static final String TOPIC_REALTIME_MARKETDATA = "topic.marketdata.realtime";
  public static final String TOPIC_REALTIME_BROKER_MARKETDATA = "topic.marketdata.brokerfeed.realtime";
  public static final String TOPIC_COMMANDS = "topic.commands";
  public static final String TOPIC_NOTIFICATIONS = "topic.notifications";
  public static final String QUEUE_MODEL_DATA = "queue.modeldata";
  public static final String QUEUE_RETRO_MODEL_DATA = "queue.retromodeldata";
  
  public static final String HISTORICL_MARKETDATA_BUS = "historical";
  public static final String REALTIME_MARKETDATA_BUS = "realtime";
  public static final String BROKER_MARKETDATA_BUS = "brokerfeed";
  public static final String COMMANDS_BUS = "commands";
  public static final String NOTIFICATIONS_BUS = "notification";
  public static final String RETRO_MODEL_DATA_BUS = "retroModelData";
  public static final String MODEL_DATA_BUS = "modelData";

}
