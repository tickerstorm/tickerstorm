package io.tickerstorm.strategy.processor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.collections.Sets;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.strategy.util.FieldUtil;

public class TestNumericChangeProcessor {

  private NumericChangeProcessor bolt;

  @Mock
  private AsyncEventBus eventBus;

  private final String stream = "TestNumericChangeBolt";

  @BeforeMethod
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    bolt = new NumericChangeProcessor();
    bolt = Mockito.spy(bolt);
    Mockito.doNothing().when(eventBus).post(Mockito.any(Field.class));
    bolt.eventBus = eventBus;
    bolt.configure(stream, Maps.newHashMap());
    bolt.configuration(stream).put(NumericChangeProcessor.PERIODS_CONFIG_KEY, "2");
  }

  @Test
  public void testComputeFirstDiscreteChange() throws Exception {

    Candle md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Candle.MIN_1_INTERVAL, Integer.MAX_VALUE);
    md.stream = stream;

    for (Field<?> f : md.getFields()) {
      bolt.handle(f);
    }

    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atLeast(1)).publish(emit.capture());

    List<Field> args = emit.getAllValues();

    Assert.assertNotNull(args);
    Assert.assertEquals(args.size(), 10);

    Field<BigDecimal> abs = (Field<BigDecimal>) args.get(0);
    Field<BigDecimal> pct = (Field<BigDecimal>) args.get(1);

    BaseField<BigDecimal> f1 =
        new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.ABS_CHANGE.field() + "-p2", BigDecimal.class);
    BaseField<BigDecimal> f2 =
        new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.PCT_CHANGE.field() + "-p2", BigDecimal.class);

    Assert.assertTrue(args.contains(f1));
    Assert.assertTrue(args.contains(f2));

  }

  @Test
  public void testComputeSecondDiscreteChange() throws Exception {


    Candle md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Candle.MIN_1_INTERVAL, Integer.MAX_VALUE);
    md.stream = stream;

    for (Field<?> f : md.getFields()) {
      bolt.handle(f);
    }

    md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Candle.MIN_1_INTERVAL,
        12335253);
    md.stream = stream;

    for (Field<?> f : md.getFields()) {
      bolt.handle(f);
    }

    md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Candle.MIN_1_INTERVAL,
        11145345);
    md.stream = stream;

    for (Field<?> f : md.getFields()) {
      bolt.handle(f);
    }

    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atLeast(3)).publish(emit.capture());

    List<Field> args = emit.getAllValues();

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.ABS_CHANGE.field() + "-p2",
        BigDecimal.valueOf(11145345 - Integer.MAX_VALUE));

    BaseField<BigDecimal> f2 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.PCT_CHANGE.field() + "-p2",
        BigDecimal.valueOf(11145345 - Integer.MAX_VALUE).divide(BigDecimal.valueOf(Integer.MAX_VALUE), 4, BigDecimal.ROUND_HALF_UP));

    for (Field<Number> f : args) {
      System.out.println(f);
    }

    Assert.assertTrue(args.contains(f1));
    Assert.assertTrue(args.contains(f2));

  }

  @Test
  public void testComputeNDiscreteChange() throws Exception {

    // Prime cache with prior tuple's fields
    List<Candle> cs = TestDataFactory.buildCandles(10, "GOOG", stream, new BigDecimal(10.19));

    List<Field<?>> fs = Lists.newArrayList();
    cs.stream().forEach(fn -> fs.addAll(fn.getFields()));
    fs.stream().filter(fn -> fn.getFieldType().isAssignableFrom(BigDecimal.class)).forEach(f -> bolt.cache(f, 2));

    // Execute bolt
    cs = TestDataFactory.buildCandles(1, "GOOG", stream, BigDecimal.ONE);

    Set<Field<?>> os = Sets.newSet();
    cs.stream().forEach(fn -> os.addAll(fn.getFields()));
    Set<Field<BigDecimal>> bs = FieldUtil.findFieldsByType(os, BigDecimal.class);


    for (Field<?> f : bs) {
      bolt.handle(f);
    }

    // Validate results
    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atLeast(1)).publish(emit.capture());

    List<Field> args = emit.getAllValues();
    Assert.assertEquals(args.size(), 8);
    Assert.assertNotNull(args.get(0));

  }

}
