/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.data.dao.influxdb;

import com.google.common.collect.Sets;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Field.Name;
import io.tickerstorm.common.entity.MarketData;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.QueryResult.Series;

/**
 * Created by kkarski on 4/10/17.
 */
public class InfluxModelDataDto {

  public static final String PREFIX = "stream|";
  private final Set<Field<?>> fields = new HashSet<>();
  private final Set<Point> points = new HashSet<>();

  public static InfluxModelDataDto convert(Field<?> field) {

    InfluxModelDataDto dto = new InfluxModelDataDto();

    Optional<Point> p = buildPoint(field);
    if (p.isPresent()) {
      dto.getPoints().add(p.get());
    }

    dto.getFields().add(field);
    return dto;
  }

  private static Optional<Point> buildPoint(Field<?> field) {

    if (!field.getName().equalsIgnoreCase(Name.SYMBOL.field()) && !field.getName().equalsIgnoreCase(Name.INTERVAL.field()) && !field.getName()
        .equalsIgnoreCase(Name.TYPE.field()) && !field.getName().equalsIgnoreCase(Name.TIMESTAMP.field()) && !field.getName()
        .equalsIgnoreCase(Name.STREAM.field())) {

      Builder builder = Point.measurement(PREFIX + field.getStream().toLowerCase())
          .time(field.getTimestamp().toEpochMilli(), TimeUnit.MILLISECONDS)
          .tag(Name.SYMBOL.field(), field.getSymbol().toLowerCase())
          .tag(Name.INTERVAL.field(), field.getInterval());

      if (String.class.equals(field.getFieldType())) {
        builder.addField(field.getName().toLowerCase(), (String) field.getValue());
      } else if (BigDecimal.class.equals(field.getFieldType()) || Integer.class.equals(field.getFieldType())) {
        builder.addField(field.getName().toLowerCase(), ((BigDecimal) field.getValue()).doubleValue());
      } else if (Integer.class.equals(field.getFieldType())) {
        builder.addField(field.getName().toLowerCase(), ((BigDecimal) field.getValue()).doubleValue());
      } else if (Instant.class.equals(field.getFieldType())) {
        builder.addField(field.getName().toLowerCase(), ((Instant) field.getValue()).toString());
      } else {
        throw new IllegalArgumentException("Unknown data type. Only Integer, BigDecimal, String, Instant accepted");
      }

      return Optional.of(builder.build());
    }

    return Optional.empty();
  }

  public static Set<InfluxModelDataDto> convertBars(Collection<Bar> bars) {

    Set<InfluxModelDataDto> dtos = Sets.newHashSet();

    bars.stream().map(b -> {
      return b.getFields();
    }).map(f -> {
      return InfluxModelDataDto.convert(f);
    }).forEach(s -> {
      dtos.add(s);
    });

    return dtos;
  }

  public static InfluxModelDataDto convert(final MarketData md) {
    return convert(md.getFields());
  }

  public static InfluxModelDataDto convert(Collection<Field<?>> fields) {

    final InfluxModelDataDto dto = new InfluxModelDataDto();
    dto.getFields().addAll(fields);

    fields.stream().map(f -> {
      return buildPoint(f);
    }).forEach(p -> {

      if (p.isPresent()) {
        dto.getPoints().add(p.get());
      }

    });

    return dto;
  }

  public static List<InfluxModelDataDto> convert(Series series) {

    List<InfluxModelDataDto> dtos = new ArrayList<>();

    final String stream = StringUtils.substringAfter(series.getName(), InfluxModelDataDto.PREFIX);
    final int symbol = series.getColumns().indexOf(Name.SYMBOL.field());
    final int interval = series.getColumns().indexOf(Name.INTERVAL.field());
    final int time = series.getColumns().indexOf("time");

    series.getValues().stream().forEach(row -> {

      final InfluxModelDataDto dto = new InfluxModelDataDto();
      final Instant timestamp = Instant.parse((String) row.get(time));
      final String symbolString = (String) row.get(symbol);
      final String intervalString = interval > -1 ? (String) row.get(interval) : "";

      dto.getFields().add(new BaseField<String>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), Name.INTERVAL.field(), intervalString));
      dto.getFields().add(new BaseField<String>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), Name.STREAM.field(), stream));
      dto.getFields().add(new BaseField<Instant>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), Name.TIMESTAMP.field(), timestamp));
      dto.getFields().add(new BaseField<String>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), Name.SYMBOL.field(), symbolString));

      for (int col = 0; col < series.getColumns().size(); col++) {

        final String fieldName = series.getColumns().get(col);

        if (col == symbol || col == interval || col == time || row.get(col) == null) {
          continue;
        }

        final Object val = row.get(col);

        switch (val.getClass().getSimpleName()) {
          case "String":

            String s = (String) val;

            try {

              Instant i = Instant.parse(s);
              dto.getFields().add(new BaseField<Instant>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), fieldName, i));

            } catch (Exception e) {

              dto.getFields().add(new BaseField<String>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), fieldName, s));

            }

          case "Double":

            Double d = (Double) val;

            dto.getFields().add(new BaseField<BigDecimal>(Bar.buildEventId(stream, symbolString, timestamp, intervalString), fieldName,
                BigDecimal.valueOf((Double) row.get(col))));

        }

      }
      dtos.add(dto);
    });

    return dtos;
  }

  public Set<Field<?>> asFields() {
    return this.getFields();
  }

  public Set<Field<?>> getFields() {
    return fields;
  }

  public Set<Point> getPoints() {
    return points;
  }
}
