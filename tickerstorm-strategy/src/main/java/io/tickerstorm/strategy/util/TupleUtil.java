package io.tickerstorm.strategy.util;

import io.tickerstorm.strategy.bolt.Fields;

import java.util.List;

import backtype.storm.tuple.Tuple;

public class TupleUtil {

  public static List<Object> propagateTuple(Tuple tuple, List<Object> output) {

    for (String key : Fields.marketdataFields()) {

      try {
        Object o = tuple.getValueByField(key);
        output.add(o);
      } catch (IllegalArgumentException e) {
        // nothing
      }

    }

    return output;

  }

}
