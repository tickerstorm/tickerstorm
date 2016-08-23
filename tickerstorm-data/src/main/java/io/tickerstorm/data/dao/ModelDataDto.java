package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.cassandra.mapping.Table;

import com.google.common.collect.Sets;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;

@Table("modeldata")
@SuppressWarnings("serial")
public class ModelDataDto implements Serializable {

  public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("uuuuMMdd").withZone(ZoneId.of("UTC"));

  @org.springframework.data.cassandra.mapping.PrimaryKey
  public ModelDataPrimaryKey primarykey;
  public final Set<String> fields = new HashSet<>();

  public Set<Field<?>> asFields() {

    Set<Field<?>> fs = new HashSet<>();

    fields.stream().forEach(s -> {
      fs.add(Field.deserialize(s));
    });

    return fs;

  }

  public static ModelDataDto convert(Field<?> field) {

    ModelDataDto dto = new ModelDataDto();

    ModelDataPrimaryKey key = new ModelDataPrimaryKey();
    key.timestamp = Date.from(field.getTimestamp());
    key.date = new BigInteger(dateFormatter.format(field.getTimestamp()));
    key.stream = field.getStream();
    dto.primarykey = key;
    dto.fields.add(field.serialize());

    return dto;
  }

  public static Set<ModelDataDto> convert(Collection<Field<?>> fs) {

    HashSet<ModelDataDto> dtos = new HashSet<>();

    fs.stream().forEach(f -> {
      dtos.add(convert(f));
    });

    return dtos;
  }

  public static Set<ModelDataDto> convert(MarketData md) {

    ModelDataDto dto = new ModelDataDto();
    ModelDataPrimaryKey key = new ModelDataPrimaryKey();
    key.timestamp = Date.from(md.getTimestamp());
    LocalDateTime dt = LocalDateTime.ofInstant(md.getTimestamp(), ZoneOffset.UTC);
    key.date = new BigInteger(dateFormatter.format(dt));
    key.stream = md.getStream();
    dto.primarykey = key;
    md.getFields().forEach(f -> {
      dto.fields.add(f.serialize());
    });

    return Sets.newHashSet(dto);
  }

}
