package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.cassandra.mapping.Table;

import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.model.Fields;

@Table("modeldata")
@SuppressWarnings("serial")
public class ModelDataDto implements Serializable {

  private static final java.time.format.DateTimeFormatter dateFormat =
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZ");

  public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("uuuuMMdd");

  @org.springframework.data.cassandra.mapping.PrimaryKey
  public ModelDataPrimaryKey primarykey;

  public Set<String> fields = new HashSet<String>();

  public Map<String, Object> fromRow() {

    Map<String, Object> row = new HashMap<>();
    row.put(Fields.MODEL_NAME.toString(), primarykey.modelName);

    for (String k : fields) {

      String[] parts = StringUtils.split(k, "$");

      String collectionName = null;
      String field = null;

      if (parts.length > 1) {
        collectionName = parts[0];
        field = parts[1];
      } else {
        field = parts[0];
      }

      Field<?> f = Field.deserialize(field);

      if (StringUtils.isEmpty(collectionName) && !row.containsKey(collectionName))

        row.put(k, f);

      else if (!StringUtils.isEmpty(collectionName) && row.containsKey(collectionName))

        ((Collection<Field<?>>) row.get(collectionName)).add(f);

      else if (!StringUtils.isEmpty(collectionName) && !row.containsKey(collectionName))

        row.put(collectionName, Lists.newArrayList(f));
    }

    if (row.containsKey(Fields.MARKETDATA.toString())) {
      Collection<Field<?>> mds = (Collection) row.get(Fields.MARKETDATA.toString());
      row.put(Fields.MARKETDATA.toString(), MarketData.build(mds.toArray(new Field[] {}))); // replace
    }

    return row;

  }

  public static ModelDataDto convert(Map<String, Object> data) {

    MarketData md = (MarketData) data.get(Fields.MARKETDATA.toString());
    String modelName = (String) data.get(Fields.MODEL_NAME.toString());

    ModelDataDto dto = new ModelDataDto();
    ModelDataPrimaryKey key = new ModelDataPrimaryKey();

    key.timestamp = Date.from(md.getTimestamp());
    LocalDateTime dt = LocalDateTime.ofInstant(md.getTimestamp(), ZoneOffset.UTC);
    key.date = Integer.valueOf(dateFormatter.format(dt));
    key.modelName = modelName;
    dto.primarykey = key;

    for (String f : data.keySet()) {

      Object o = data.get(f);

      if (o == null)
        continue;

      if (MarketData.class.isAssignableFrom(o.getClass()) && !Marker.class.isAssignableFrom(o.getClass())) {

        for (Field<?> mf : ((MarketData) o).getFields()) {
          dto.fields.add(f + "$" + mf.serialize());
        }

      } else if (Field.class.isAssignableFrom(o.getClass())) {

        dto.fields.add(((Field<?>) o).serialize());

      } else if (Collection.class.isAssignableFrom(o.getClass())) {

        for (Object i : (Collection<?>) o) {
          if (Field.class.isAssignableFrom(i.getClass())) {
            dto.fields.add(f + "$" + ((Field<?>) i).serialize());
          }
        }
      }
    }

    return dto;

  }

}
