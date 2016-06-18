package io.tickerstorm.data.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;

public class TestModelDataDto {

  private final Candle c =
      new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @BeforeMethod
  public void init(){
    c.stream = "TestModelDataDto"; 
  }
  
  @Test
  public void testSerializeDeserializeModelData() {

    Map<String, Object> fields = (Map) c.getFieldsAsMap();
    assertTrue(fields.size() > 0);
    ModelDataDto dto = ModelDataDto.convert(fields);
    assertNotNull(dto);

    Map<String, Object> fields2 = dto.fromRow();
    assertEquals(fields2.size(), fields.size());

    for (String key : fields.keySet()) {

      assertTrue(fields2.containsKey(key), key + " doesn't exist");
      Field<?> f1 = (Field<?>) fields.get(key);
      Field<?> f2 = (Field<?>) fields2.get(key);
      assertEquals(f2, f1);
    }

    assertEquals(fields, fields2);
  }

  @Test
  public void testSerializeDeserializeModelDataWithCollections() {

    Map<String, Object> fields = (Map) c.getFieldsAsMap();
    fields.put("simple-statistics", Lists.newArrayList());
    ((ArrayList<Field<?>>) fields.get("simple-statistics"))
        .add(new BaseField<BigDecimal>(c.getEventId(), Field.Name.MIN.field(), BigDecimal.ONE));


    assertTrue(fields.size() > 0);
    ModelDataDto dto = ModelDataDto.convert(fields);
    assertNotNull(dto);

    Map<String, Object> fields2 = dto.fromRow();
    assertEquals(fields2.size(), fields.size());

    boolean entered = false;
    boolean entered2 = false;
    boolean entered3 = false;
    for (String key : fields.keySet()) {
      entered = true;
      assertTrue(fields2.containsKey(key), key + " doesn't exist");

      if (Field.class.isAssignableFrom(fields.get(key).getClass())) {
        
        entered3 = true;
        Field<?> f1 = (Field<?>) fields.get(key);
        Field<?> f2 = (Field<?>) fields2.get(key);
        assertEquals(f2, f1);

      } else if (Collection.class.isAssignableFrom(fields.get(key).getClass())) {
        
        entered2 = true;
        assertEquals("simple-statistics", key);
        assertEquals(1, ((Collection<?>) fields.get(key)).size());
        assertEquals(((Collection<?>) fields.get(key)).size(), ((Collection<?>) fields2.get(key)).size());
        assertEquals(fields.get(key), fields2.get(key));
      
      }

    }
    
    assertTrue(entered);
    assertTrue(entered2);
    assertTrue(entered3);
  }

}
