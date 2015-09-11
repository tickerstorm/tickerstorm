package io.tickerstorm.strategy.bolt;

import java.util.Collections;
import java.util.List;

import org.apache.storm.guava.collect.Lists;


public interface Fields {

  public static final String MARKETDATA = "marketdata";
  public static final String CANDEL = "candel";
  public static final String QUOTE = "quote";
  public static final String TICK = "tick";
  public static final String AVERAGES = "averages";
  public static final String NOW = "now";

  public static final List<String> MARKETADATA_FIELDS = Collections.unmodifiableList(Lists
      .newArrayList(io.tickerstorm.strategy.bolt.Fields.MARKETDATA,
          io.tickerstorm.strategy.bolt.Fields.CANDEL, io.tickerstorm.strategy.bolt.Fields.QUOTE,
          io.tickerstorm.strategy.bolt.Fields.TICK));
}
