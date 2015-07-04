package io.tickerstorm.dao;

import java.time.Instant;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.dozer.CustomConverter;
import org.dozer.MappingException;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Months;
import org.joda.time.format.ISODateTimeFormat;

public class CustomDateTimeConverter implements CustomConverter {

  @Override
  public Object convert(Object dest, Object source, Class<?> destClass, Class<?> sourceClass) {

    if (source == null) {
      return null;
    }

    if (sourceClass.equals(Integer.class) && destClass.equals(Days.class)) {

      return Days.days((Integer) source);

    } else if (destClass.equals(Integer.class) && sourceClass.equals(Days.class)) {

      return new Integer(((Days) source).getDays());

    } else if (sourceClass.equals(Integer.class) && destClass.equals(Months.class)) {

      return Months.months((Integer) source);

    } else if (destClass.equals(Integer.class) && sourceClass.equals(Months.class)) {

      return new Integer(((Months) source).getMonths());

    } else if (sourceClass.equals(String.class) && destClass.equals(Months.class)) {

      if (((String) source).equalsIgnoreCase("month")) {
        return Months.ONE;
      } else if (((String) source).equalsIgnoreCase("year")) {
        return Months.TWELVE;
      }

    } else if (destClass.equals(Months.class) && sourceClass.equals(String.class)) {

      return "month";

    } else if (destClass.equals(DateTime.class) && sourceClass.equals(String.class)) {

      if (StringUtils.isEmpty((String) source) || ((String) source).equalsIgnoreCase("null")) {
        return null;
      }

      DateTime dt = null;
      dt = DateTime.parse((String) source);
      return dt;

    } else if (destClass.equals(String.class) && sourceClass.equals(DateTime.class)) {

      return ((DateTime) source).toString(ISODateTimeFormat.dateTime());

    } else if (destClass.equals(Long.class) && sourceClass.equals(DateTime.class)) {

      return ((DateTime) source).getMillis();

    } else if (destClass.equals(Long.class) && sourceClass.equals(String.class)) {

      if (((String) source).contains("0000-00-00 00:00:00") || StringUtils.isEmpty((String) source)
          || ((String) source).equalsIgnoreCase("null")) {
        return null;
      }

      DateTime dt = DateTime.parse((String) source);
      return dt.getMillis();

    } else if (destClass.equals(String.class) && sourceClass.equals(Long.class)) {

      DateTime dt = new DateTime(source);
      return dt.toString(ISODateTimeFormat.dateTime());

    } else if (destClass.equals(Long.class) && sourceClass.equals(DateTime.class)) {

      return ((DateTime) source).getMillis();

    } else if (destClass.equals(DateTime.class) && sourceClass.equals(Long.class)) {

      return new DateTime(source);

    } else if (destClass.equals(Long.class) && sourceClass.equals(Date.class)) {

      return ((Date) source).getTime();

    } else if (destClass.equals(Date.class) && sourceClass.equals(Long.class)) {

      return new Date((Long) source);

    } else if (destClass.equals(DateTime.class) && sourceClass.equals(Date.class)) {

      return new DateTime((Date) source);

    } else if (destClass.equals(Date.class) && sourceClass.equals(DateTime.class)) {

      return ((DateTime) source).toDate();

    } else if (destClass.equals(Instant.class) && sourceClass.equals(Instant.class)) {

      return source;

    } else if (destClass.equals(Instant.class) && sourceClass.equals(Date.class)) {

      return ((Date) source).toInstant();

    } else if (destClass.equals(Date.class) && sourceClass.equals(Instant.class)) {

      return Date.from((Instant) source);

    }

    throw new MappingException("Converter TestCustomConverter " + "used incorrectly. Arguments passed in were:" + dest + " and " + source);

  }
}
