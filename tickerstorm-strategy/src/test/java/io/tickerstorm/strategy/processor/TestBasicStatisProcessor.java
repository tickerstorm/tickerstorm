package io.tickerstorm.strategy.processor;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
import io.tickerstorm.common.config.SymbolConfig;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.flow.BasicStatsProcessor;

public class TestBasicStatisProcessor {

  private BasicStatsProcessor bolt;

  @Mock
  private EventBus eventBus;

  @Mock
  private GaugeService service;

  private final String stream = "TestBasicStatisProcessor".toLowerCase();

  @BeforeMethod
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    bolt = new BasicStatsProcessor();
    bolt = Mockito.spy(bolt);
    Mockito.doNothing().when(eventBus).post(Mockito.any(Field.class));
    bolt.eventBus = eventBus;

    SymbolConfig config = new SymbolConfig();
    config.symbol.add("*");
    config.interval.add("*");
    config.periods.add("2");

    bolt.getConfig(stream).put(BaseProcessor.TRANSFORMER_CONFIG_KEY,
        new TransformerConfig(com.google.common.collect.Sets.newHashSet(config)));

    bolt.gauge = service;
    Mockito.doNothing().when(service).submit(Mockito.anyString(), Mockito.anyDouble());
    CacheManager.getInstance(stream);
  }

  @AfterMethod
  public void clean() {
    Mockito.reset(bolt);
    CacheManager.getInstance(stream).removeAll();
  }

  @Test
  public void testComputeFirstDiscreteChange() throws Exception {

    Bar md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        Integer.MAX_VALUE);

    bolt.handle(md.getFields());

    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atMost(0)).publish(emit.capture());

  }

  @Test
  public void testComputeSecondDiscreteChange() throws Exception {


    Bar md1 = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        Integer.MAX_VALUE);


    bolt.handle(md1.getFields());


    Bar md2 = new Bar("TOL", stream, Instant.now().plus(5, ChronoUnit.MILLIS), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, 12335253);


    bolt.handle(md2.getFields());


    Bar md3 = new Bar("TOL", stream, Instant.now().plus(5, ChronoUnit.MILLIS), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, 11145345);


    bolt.handle(md3.getFields());

    ArgumentCaptor<Collection> emit = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(bolt, Mockito.atLeast(10)).publish(emit.capture());
    Mockito.verify(bolt, Mockito.atMost(10)).publish(emit.capture());

    Set<Field<Number>> args = new HashSet<>();

    emit.getAllValues().stream().forEach(c -> {
      args.addAll(c);
      System.out.println(c);
    });

    Assert.assertEquals(args.size(), 40);

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md2.getEventId(), "volume|" + Field.Name.MAX.field() + "-p2", new BigDecimal(Integer.MAX_VALUE).setScale(4, BigDecimal.ROUND_HALF_UP));
    BaseField<BigDecimal> f2 = new BaseField<BigDecimal>(md3.getEventId(), "volume|" + Field.Name.MIN.field() + "-p2", new BigDecimal(11145345).setScale(4, BigDecimal.ROUND_HALF_UP));
    BaseField<BigDecimal> f3 = new BaseField<BigDecimal>(md3.getEventId(), "volume|" + Field.Name.SMA.field() + "-p2", new BigDecimal(11740299).setScale(4, BigDecimal.ROUND_HALF_UP));
    BaseField<BigDecimal> f4 = new BaseField<BigDecimal>(md3.getEventId(), "volume|" + Field.Name.STD.field() + "-p2", new BigDecimal(841392.0158).setScale(4, BigDecimal.ROUND_HALF_UP));

    System.out.println(f1);
    System.out.println(f2);
    System.out.println(f3);
    System.out.println(f4);
    
    Assert.assertTrue(args.contains(f1));
    Assert.assertTrue(args.contains(f2));
    Assert.assertTrue(args.contains(f3));
    Assert.assertTrue(args.contains(f4));

  }
}
