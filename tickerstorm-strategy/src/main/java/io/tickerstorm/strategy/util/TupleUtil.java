package io.tickerstorm.strategy.util;

import static io.tickerstorm.common.entity.Field.Name.MARKETDATA;
import static io.tickerstorm.common.entity.Field.Name.NOW;
import static io.tickerstorm.common.entity.Field.Name.STREAM;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.MarketData;

public class TupleUtil {

  public static List<Object> propagateTuple(Tuple tuple, List<Object> output) {

    for (String key : marketdataFields()) {

      try {
        Object o = tuple.getValueByField(key);
        output.add(o);
      } catch (IllegalArgumentException e) {
        // nothing
      }

    }

    return output;

  }

  public static Set<String> marketdataFields() {
    return Collections.unmodifiableSet(Sets.newHashSet(MARKETDATA.field(), NOW.field(), STREAM.field()));
  }

  public static Map<String, Field<?>> flattenFields(Tuple tuple) {

    Map<String, Field<?>> columns = new HashMap<>();
    backtype.storm.tuple.Fields fields = tuple.getFields();

    for (String f : fields) {

      Object o = tuple.getValueByField(f);

      if (o == null)
        continue;

      if (MarketData.class.isAssignableFrom(o.getClass()) && !Marker.class.isAssignableFrom(o.getClass())) {

        for (Field<?> mf : ((MarketData) o).getFields()) {
          columns.put(f + ":" + mf.getName(), mf);
        }

      } else if (Field.class.isAssignableFrom(o.getClass())) {

        columns.put(((Field<?>) o).getName(), (Field<?>) o);

      } else if (Collection.class.isAssignableFrom(o.getClass())) {

        for (Object i : (Collection<?>) o) {
          if (Field.class.isAssignableFrom(i.getClass())) {
            columns.put(f + ":" + ((Field<?>) o).getName(), (Field<?>) o);
          }
        }
      }
    }

    return columns;
  }

  public static HashMap<String, Object> toMap(Tuple tuple) {

    HashMap<String, Object> columns = new HashMap<>();
    backtype.storm.tuple.Fields fields = tuple.getFields();

    for (String f : fields) {

      Object o = tuple.getValueByField(f);

      if (o == null)
        continue;

      if ((MarketData.class.isAssignableFrom(o.getClass()) && !Marker.class.isAssignableFrom(o.getClass()))
          || (Field.class.isAssignableFrom(o.getClass())) || (Collection.class.isAssignableFrom(o.getClass()))) {
        columns.put(f, o);
      }
    }

    return columns;

  }

  public static List<Field<?>> listFields(Tuple tuple) {

    List<Field<?>> columns = new ArrayList<>();
    backtype.storm.tuple.Fields fields = tuple.getFields();

    for (String f : fields) {

      Object o = tuple.getValueByField(f);

      if (o == null)
        continue;

      if (MarketData.class.isAssignableFrom(o.getClass()) && !Marker.class.isAssignableFrom(o.getClass())) {

        columns.addAll(((MarketData) o).getFields());

      } else if (Field.class.isAssignableFrom(o.getClass())) {

        columns.add((Field<?>) o);

      } else if (Collection.class.isAssignableFrom(o.getClass())) {

        for (Object i : (Collection<?>) o) {
          if (Field.class.isAssignableFrom(i.getClass())) {
            columns.add((Field<?>) i);
          }
        }
      }
    }

    return columns;

  }

  public static List<Field<?>> sortFields(Collection<Field<?>> fields) {

    List<Field<?>> columns = new ArrayList<>(fields);

    columns.sort(new Comparator<Field<?>>() {
      @Override
      public int compare(Field<?> o1, Field<?> o2) {
        return (o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase()));
      }
    });

    return columns;

  }

}
