package io.tickerstorm.common.data.converter;

import java.math.BigDecimal;
import java.time.Instant;

import org.apache.commons.lang3.StringUtils;

public class Util {
  
  public static <T> T convert(String value, Class<T> clazz) {

    if (String.class.isAssignableFrom(clazz))
      return (T) value;
    else if (Integer.class.isAssignableFrom(clazz))
      return (T) Integer.valueOf(value);
    else if (BigDecimal.class.isAssignableFrom(clazz))
      return (T) new BigDecimal(value);
    else if (Instant.class.isAssignableFrom(clazz))
      return (T) Instant.parse(value);
    else if (StringUtils.isEmpty(value))
      return null;

    throw new IllegalArgumentException("Unknown type " + clazz + " for value " + value);
  }

}
