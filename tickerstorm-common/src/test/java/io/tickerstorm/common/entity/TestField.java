package io.tickerstorm.common.entity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBaseField {

  private final Candle c =
      new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @Test
  public void testEventIdGeneration() {

    c.setStream("default");
    Map<String, Field<?>> fields = c.getFieldsAsMap();

    Field<BigDecimal> vol = (Field<BigDecimal>) fields.get(Field.Name.VOLUME.field());
    Assert.assertEquals(vol.getEventId(), c.getEventId());


  }

}
