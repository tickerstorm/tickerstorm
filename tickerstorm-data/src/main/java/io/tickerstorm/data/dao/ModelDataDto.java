package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.cassandra.mapping.Table;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.model.Fields;

@Table("modeldata")
@SuppressWarnings("serial")
public class ModelDataDto implements Serializable {

  public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("uuuuMMdd");

  @org.springframework.data.cassandra.mapping.PrimaryKey
  public ModelDataPrimaryKey primarykey;

  public Map<String, Object> fields = new HashMap<String, Object>();

  public static ModelDataDto convert(Map<String, Object> data) {

    MarketData md = (MarketData) data.get(Fields.MARKETDATA);
    String modelName = (String) data.get(Fields.MODEL_NAME);

    ModelDataDto dto = new ModelDataDto();
    ModelDataPrimaryKey key = new ModelDataPrimaryKey();

    key.timestamp = (Date) CustomDateTimeConverter.convertDateTime(null, md.getTimestamp(), Date.class, Instant.class);
    LocalDateTime dt = LocalDateTime.ofInstant(md.getTimestamp(), ZoneOffset.UTC);
    key.date = dateFormatter.format(dt);
    key.modelName = modelName;
    dto.primarykey = key;

    for (String f : data.keySet()) {

      Object o = data.get(f);

      if (o == null)
        continue;

      if (MarketData.class.isAssignableFrom(o.getClass()) && !Marker.class.isAssignableFrom(o.getClass())) {

        for (Field<?> mf : ((MarketData) o).getFields()) {
          dto.fields.put(f + ":" + mf.getName(), mf.getValue());
        }

      } else if (Field.class.isAssignableFrom(o.getClass())) {

        dto.fields.put(((Field<?>) o).getName(), ((Field<?>) o).getValue());

      } else if (Collection.class.isAssignableFrom(o.getClass())) {

        for (Object i : (Collection<?>) o) {
          if (Field.class.isAssignableFrom(i.getClass())) {
            dto.fields.put(f + ":" + ((Field<?>) o).getName(), ((Field<?>) o).getValue());
          }
        }
      }
    }

    return dto;

  }

}
