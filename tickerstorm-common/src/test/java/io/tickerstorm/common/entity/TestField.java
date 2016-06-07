package io.tickerstorm.common.entity;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import org.testng.annotations.Test;

public class TestField {

  private final Candle c =
      new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @Test
  public void testFieldParsing() {

    c.setStream("default");
    Map<String, Field<?>> fields = c.getFieldsAsMap();

    Field<Integer> vol = (Field<Integer>) fields.get(Field.Name.VOLUME.field());
    assertEquals(vol.getEventId(), c.getEventId());
    assertEquals(vol.getName(), Field.Name.VOLUME.field());
    assertEquals(vol.getStream(), c.getStream());
    assertEquals(Field.parseType(vol.serialize()).getName(), vol.getValue().getClass().getName());
    assertEquals(Field.parseField(vol.serialize()), vol.getName());
    assertEquals(Field.parseValue(vol.serialize(), Field.Name.VOLUME.fieldType()), vol.getValue());
    assertEquals(Field.parseStream(vol.serialize()), vol.getStream());
    assertEquals(Field.parseEventId(vol.serialize()), vol.getEventId());

  }

  @Test
  public void testFieldDeserialization() {

    c.setStream("default");
    Map<String, Field<?>> fields = c.getFieldsAsMap();

    Field<Integer> vol = (Field<Integer>) fields.get(Field.Name.VOLUME.field());

    String serial = vol.serialize();

    Field<Integer> f = (Field<Integer>) Field.deserialize(serial);

    assertEquals(f.getEventId(), c.getEventId());
    assertEquals(f.getName(), Field.Name.VOLUME.field());
    assertEquals(f.getStream(), c.getStream());
    assertEquals(f.getValue(), c.getVolume());
    assertEquals(f.getFieldType().getName(), Integer.class.getName());
    assertEquals(f.getFieldType(), Field.Name.VOLUME.fieldType());
  }
  
  @Test
  public void testSerializeNullField(){
    
    c.setStream("default");
    Map<String, Field<?>> fields = c.getFieldsAsMap();
    Field<Integer> vol = (Field<Integer>) fields.get(Field.Name.VOLUME.field());
    
    Field<Integer> nullField = new BaseField<Integer>(vol, Field.Name.MAX.field() + "-p" + 5, Integer.class);
    String serialized = nullField.serialize();
    
    Field<Integer> newNullField = (Field<Integer>) Field.deserialize(serialized);
    assertEquals(nullField, newNullField);
  }

}
