package io.tickerstorm.strategy.util;

import static io.tickerstorm.common.entity.Field.Name.MARKETDATA;
import static io.tickerstorm.common.entity.Field.Name.NOW;
import static io.tickerstorm.common.entity.Field.Name.STREAM;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import io.tickerstorm.common.entity.Field;

public class FieldUtil {

  public static Set<String> marketdataFields() {
    return Collections.unmodifiableSet(Sets.newHashSet(MARKETDATA.field(), NOW.field(), STREAM.field()));
  }

  public static <T> Set<Field<T>> findFieldsByType(Set<Field<?>> fs, Class<T> clazz) {

    Set<Field<T>> filtered =
        fs.stream().parallel().filter(fn -> fn.getFieldType().isAssignableFrom(clazz)).map(fn -> (Field<T>) fn).collect(Collectors.toSet());
    return new HashSet<Field<T>>(filtered);

  }

  public static Set<Field<?>> mapToFields(Collection<?> fs) {

    Set<Field<?>> filtered = fs.stream().parallel().filter(fn -> fn.getClass().isAssignableFrom(Field.class)).map(fn -> (Field<?>) fn)
        .collect(Collectors.toSet());
    return filtered;

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
  
  public static Field<?> fetch(List<Field<?>> previous, Field<?> from, int periods) {
   
    Field<?> prior = from;

    if (previous != null && !previous.isEmpty() && previous.size() > periods) {

      int index = previous.indexOf(from);
      if ((index - periods) >= 0) {
        prior = previous.get(index - periods);
      }
    }

    return prior;

  }

}
