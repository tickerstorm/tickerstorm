package io.tickerstorm.data.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;

public class TestModelDataDto {

  private final Bar c =
      new Bar("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @BeforeMethod
  public void init() {
    c.stream = "TestModelDataDto";
  }

  @Test
  public void testSerializeDeserializeModelData() {

    Set<Field<?>> fields = c.getFields();
    assertEquals(fields.size(), 9);

    Set<ModelDataDto> dtos = ModelDataDto.convert(fields);
    assertNotNull(dtos);
    assertEquals(dtos.size(), 9);

    assertTrue(dtos.stream().allMatch(d -> {

      return fields.containsAll(d.asFields());

    }));

  }
}
