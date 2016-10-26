package io.tickerstorm.common.entity;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import org.testng.annotations.Test;

public class TestCandle {

  private final Bar c =
      new Bar("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @Test
  public void testFieldGeneration() {

    c.setStream("default");
    Map<String, Field<?>> fields = c.getFieldsAsMap();

    Field<BigDecimal> vol = (Field<BigDecimal>) fields.get(Field.Name.VOLUME.field());
    assertEquals(vol.getEventId(), c.getEventId());
    assertEquals(vol.getName(), Field.Name.VOLUME.field());
    assertEquals(vol.getStream(), c.getStream());
    assertEquals(vol.getValue(), c.getVolume());
    assertEquals(Bar.parseInterval(c.getEventId()), c.getInterval());
    assertEquals(MarketData.parseSymbol(c.getEventId()), c.getSymbol());
    assertEquals(MarketData.parseTimestamp(c.getEventId()), c.getTimestamp());

  }

}
