/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.strategy.processor;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.config.SymbolConfig;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.strategy.processor.flow.BasicStatsProcessor;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.math3.stat.descriptive.ComparableDescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.metrics.GaugeService;

public class TestBasicStatisProcessor {

  private final String stream = "TestBasicStatisProcessor".toLowerCase();

  private BasicStatsProcessor bolt;

  @Mock
  private EventBus eventBus;

  @Mock
  private GaugeService service;

  @Before
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

  @After
  public void clean() {
    Mockito.reset(bolt);
    CacheManager.getInstance(stream).removeAll();
  }

  @Test
  public void testComputeFirstDiscreteChange() throws Exception {

    Bar md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        new BigDecimal(Integer.MAX_VALUE));

    bolt.handle(md.getFields());

    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atMost(0)).publish(emit.capture());

  }

  @Test
  public void testComputeSecondDiscreteChange() throws Exception {

    Bar md1 = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        new BigDecimal(Integer.MAX_VALUE));

    bolt.handle(md1.getFields());

    Bar md2 = new Bar("TOL", stream, Instant.now().plus(5, ChronoUnit.MILLIS), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, new BigDecimal(12335253));

    bolt.handle(md2.getFields());

    Bar md3 = new Bar("TOL", stream, Instant.now().plus(5, ChronoUnit.MILLIS), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, new BigDecimal(11145345));

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

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md2.getEventId(), "volume|" + Field.Name.MAX.field() + "-p2",
        new BigDecimal(Integer.MAX_VALUE).setScale(4, BigDecimal.ROUND_HALF_UP));
    BaseField<BigDecimal> f2 = new BaseField<BigDecimal>(md3.getEventId(), "volume|" + Field.Name.MIN.field() + "-p2",
        new BigDecimal(11145345).setScale(4, BigDecimal.ROUND_HALF_UP));
    BaseField<BigDecimal> f3 = new BaseField<BigDecimal>(md3.getEventId(), "volume|" + Field.Name.SMA.field() + "-p2",
        new BigDecimal(11740299).setScale(4, BigDecimal.ROUND_HALF_UP));
    BaseField<BigDecimal> f4 = new BaseField<BigDecimal>(md3.getEventId(), "volume|" + Field.Name.STD.field() + "-p2",
        new BigDecimal(841392.0158).setScale(4, BigDecimal.ROUND_HALF_UP));

    System.out.println(f1);
    System.out.println(f2);
    System.out.println(f3);
    System.out.println(f4);

    Assert.assertTrue(args.contains(f1));
    Assert.assertTrue(args.contains(f2));
    Assert.assertTrue(args.contains(f3));
    Assert.assertTrue(args.contains(f4));

  }

  @Test
  public void testBasicStatsProcessorConcurrently() {

    bolt.eventBus = new EventBus();
    final AtomicInteger counter = new AtomicInteger(0);

    bolt.eventBus.register(new Object() {
      @Subscribe
      public void onMessage(Set<Field<?>> fs) {

        counter.incrementAndGet();
      }
    });

    TestDataFactory.buildCandles(2, "goog", stream, new BigDecimal("34.53")).stream().parallel().forEach(b -> {
      bolt.handle(b.getFields());
    });

    Assert.assertEquals(5, counter.get());

  }

  @Test
  public void compareDescriptiveStatistics() {

    SynchronizedDescriptiveStatistics d = new SynchronizedDescriptiveStatistics(2);
    SynchronizedDescriptiveStatistics d2 = new SynchronizedDescriptiveStatistics(2);
    SynchronizedDescriptiveStatistics d3 = new SynchronizedDescriptiveStatistics(3);
    SynchronizedDescriptiveStatistics d4 = new SynchronizedDescriptiveStatistics(2);
    ComparableDescriptiveStatistics d5 = new ComparableDescriptiveStatistics(2);
    ComparableDescriptiveStatistics d6 = new ComparableDescriptiveStatistics(3);
    ComparableDescriptiveStatistics d7 = new ComparableDescriptiveStatistics(3);

    d.addValue(3d);
    d2.addValue(2d);
    d3.addValue(2d);
    d4.addValue(3d);
    d5.addValue(3d);
    d6.addValue(3d);
    d7.addValue(3d);

    Assert.assertEquals(d, d);
    Assert.assertEquals(d5, d);
    Assert.assertNotEquals(d5, d6);
    Assert.assertEquals(d7, d6);

    Assert.assertNotEquals(d, d2);
    Assert.assertNotEquals(d2, d3);

  }

//  @Test
//  public void testCacheReplaceCompare() {
//
//    CacheManager.getInstance("test1").putIfAbsent(new Element("p1", new ComparableDescriptiveStatistics(2)));
//    final Element e1 = CacheManager.getInstance("test1").get("p1");
//    Element e2 = CacheManager.getInstance("test1").get("p1");
//
//    ComparableDescriptiveStatistics q1 = (ComparableDescriptiveStatistics) e1.getObjectValue();
//    ComparableDescriptiveStatistics q2 = (ComparableDescriptiveStatistics) e2.getObjectValue();
//
//    q1.addValue(2d);
//    q2.addValue(2d);
//
//    Assert.assertTrue(CacheManager.getInstance("test1").replace(e1, new Element("p1", q1)));
//    Assert.assertFalse(CacheManager.getInstance("test1").replace(e2, new Element("p1", q2)));
//
//    e2 = CacheManager.getInstance("test1").get("p1");
//    q2 = (ComparableDescriptiveStatistics) e2.getObjectValue();
//    q2.addValue(2d);
//
//    Assert.assertTrue(CacheManager.getInstance("test1").replace(e2, new Element("p1", q2)));
//
//  }
}



