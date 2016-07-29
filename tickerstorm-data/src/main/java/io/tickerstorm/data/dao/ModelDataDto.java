package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
import io.tickerstorm.common.entity.MarketData;

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
    row.put(Field.Name.STREAM.field(), primarykey.stream);

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

        row.put(f.getName(), f);

      else if (!StringUtils.isEmpty(collectionName) && row.containsKey(collectionName))

        ((Collection<Field<?>>) row.get(collectionName)).add(f);

      else if (!StringUtils.isEmpty(collectionName) && !row.containsKey(collectionName))

        row.put(collectionName, Lists.newArrayList(f));
    }

    if (row.containsKey(Field.Name.MARKETDATA.field())) {
      Collection<Field<?>> mds = (Collection) row.get(Field.Name.MARKETDATA.field());
      row.put(Field.Name.MARKETDATA.field(), MarketData.build(new HashSet<>(mds))); // replace
    }

    return row;

  }

  public static ModelDataDto convert(Map<String, Object> data) {

    ModelDataDto dto = new ModelDataDto();

    for (String f : data.keySet()) {

      Object o = data.get(f);

      if (o == null)
        continue;

      if (MarketData.class.isAssignableFrom(o.getClass())) {

        MarketData md = (MarketData) o;

        ModelDataPrimaryKey key = new ModelDataPrimaryKey();
        key.timestamp = Date.from(md.getTimestamp());
        LocalDateTime dt = LocalDateTime.ofInstant(md.getTimestamp(), ZoneOffset.UTC);
        key.date = Integer.valueOf(dateFormatter.format(dt));
        key.stream = md.getStream();
        dto.primarykey = key;

        for (Field<?> mf : ((MarketData) o).getFields()) {
          dto.fields.add(f + "$" + mf.serialize());
        }

      } else if (Field.class.isAssignableFrom(o.getClass())) {

        Field<?> field = (Field<?>) o;

        ModelDataPrimaryKey key = new ModelDataPrimaryKey();
        key.timestamp = Date.from(MarketData.parseTimestamp(field.getEventId()));
        LocalDateTime dt = LocalDateTime.ofInstant(key.timestamp.toInstant(), ZoneOffset.UTC);
        key.date = Integer.valueOf(dateFormatter.format(dt));
        key.stream = field.getStream();
        dto.primarykey = key;

        dto.fields.add(field.serialize());

      } else if (Collection.class.isAssignableFrom(o.getClass())) {

        for (Object i : (Collection<?>) o) {

          if (Field.class.isAssignableFrom(i.getClass())) {

            Field<?> field = (Field<?>) i;

            ModelDataPrimaryKey key = new ModelDataPrimaryKey();
            key.timestamp = Date.from(MarketData.parseTimestamp(field.getEventId()));
            LocalDateTime dt = LocalDateTime.ofInstant(key.timestamp.toInstant(), ZoneOffset.UTC);
            key.date = Integer.valueOf(dateFormatter.format(dt));
            key.stream = field.getStream();
            dto.primarykey = key;
            dto.fields.add(f + "$" + field.serialize());

          } else if (MarketData.class.isAssignableFrom(i.getClass())) {

            MarketData md = (MarketData) i;

            ModelDataPrimaryKey key = new ModelDataPrimaryKey();
            key.timestamp = Date.from(md.getTimestamp());
            LocalDateTime dt = LocalDateTime.ofInstant(md.getTimestamp(), ZoneOffset.UTC);
            key.date = Integer.valueOf(dateFormatter.format(dt));
            key.stream = md.getStream();
            dto.primarykey = key;

            for (Field<?> mf : md.getFields()) {
              dto.fields.add(f + "$" + mf.serialize());
            }
          }
        }
      }
    }


    if (dto.primarykey != null)
      return dto;

    return null;

  }

}
