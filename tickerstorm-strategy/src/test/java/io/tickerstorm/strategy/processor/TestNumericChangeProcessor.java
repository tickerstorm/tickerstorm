package io.tickerstorm.strategy.processor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.metrics.GaugeService;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.strategy.processor.flow.NumericChangeProcessor;

public class TestNumericChangeProcessor {

  private NumericChangeProcessor bolt;

  @Mock
  private EventBus eventBus;

  @Mock
  private GaugeService service;

  private final String stream = "TestNumericChangeBolt".toLowerCase();

  @BeforeMethod
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    bolt = new NumericChangeProcessor();
    bolt = Mockito.spy(bolt);
    Mockito.doNothing().when(eventBus).post(Mockito.any(Field.class));
    bolt.eventBus = eventBus;
    bolt.getConfig(stream).put(NumericChangeProcessor.PERIODS_CONFIG_KEY, "2");

    bolt.gauge = service;
    Mockito.doNothing().when(service).submit(Mockito.anyString(), Mockito.anyDouble());

    Bar md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, Integer.MAX_VALUE);
    CacheManager.cache(new BaseField<>(md.getEventId(), "warm cache", BigDecimal.class));
    CacheManager.getInstance(stream).removeAll();
  }

  @AfterMethod
  public void clean() {
    Mockito.reset(bolt);
    CacheManager.getInstance(stream).removeAll();
  }

  @Test
  public void testComputeFirstDiscreteChange() throws Exception {

    Bar md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, Integer.MAX_VALUE);

    bolt.handle(md.getFields());

    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atMost(0)).publish(emit.capture());

  }

  @Test
  public void testComputeSecondDiscreteChange() throws Exception {


    Bar md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, Integer.MAX_VALUE);


    bolt.handle(md.getFields());


    md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        12335253);


    bolt.handle(md.getFields());


    md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        11145345);


    bolt.handle(md.getFields());

    ArgumentCaptor<Collection> emit = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(bolt).publish(emit.capture());

    Set<Field<Number>> args = new HashSet<>();
    emit.getAllValues().stream().forEach(c -> {
      args.addAll(c);
    });

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.ABS_CHANGE.field() + "-p2",
        BigDecimal.valueOf(11145345 - Integer.MAX_VALUE).setScale(4, BigDecimal.ROUND_HALF_UP));

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
    List<Bar> cs = TestDataFactory.buildCandles(10, "GOOG", stream, new BigDecimal(10.19));

    cs.stream().forEach(c -> {
      c.getFields().stream().filter(f -> f.getFieldType().isAssignableFrom(BigDecimal.class)).forEach(f -> {
        CacheManager.cache(f);
      });
    });


    cs = TestDataFactory.buildCandles(1, "GOOG", stream, BigDecimal.ONE);

    Set<Field<?>> os =
        cs.get(0).getFields().stream().filter(f -> f.getFieldType().isAssignableFrom(BigDecimal.class)).collect(Collectors.toSet());

    // Execute bolt
    bolt.handle(os);

    // Validate results
    ArgumentCaptor<Collection> emit = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(bolt, Mockito.atLeast(1)).publish(emit.capture());

    List<Collection> args = emit.getAllValues();
    Assert.assertEquals(args.get(0).size(), 8);

  }

}
