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

import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.config.SymbolConfig;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.strategy.processor.flow.NumericChangeProcessor;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.metrics.GaugeService;

public class TestNumericChangeProcessor {

  private NumericChangeProcessor bolt;

  @Mock
  private EventBus eventBus;

  @Mock
  private GaugeService service;

  private final String stream = "TestNumericChangeBolt".toLowerCase();

  @Before
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    bolt = new NumericChangeProcessor();
    bolt = Mockito.spy(bolt);
    Mockito.doNothing().when(eventBus).post(Mockito.any(Field.class));
    bolt.eventBus = eventBus;

    SymbolConfig config = new SymbolConfig();
    config.symbol.add("*");
    config.interval.add("*");
    config.periods.add("2");

    bolt.getConfig(stream).put(BaseProcessor.TRANSFORMER_CONFIG_KEY,
        new TransformerConfig(Sets.newHashSet(config)));

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
        Integer.MAX_VALUE);

    bolt.handle(md.getFields());

    ArgumentCaptor<Field> emit = ArgumentCaptor.forClass(Field.class);
    Mockito.verify(bolt, Mockito.atMost(0)).publish(emit.capture());

  }

  @Test
  public void testComputeSecondDiscreteChange() throws Exception {


    Bar md = new Bar("TOL", stream, Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Bar.MIN_1_INTERVAL,
        Integer.MAX_VALUE);


    bolt.handle(md.getFields());


    md = new Bar("TOL", stream, Instant.now().plus(5, ChronoUnit.MILLIS), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, 12335253);


    bolt.handle(md.getFields());


    md = new Bar("TOL", stream, Instant.now().plus(5, ChronoUnit.MILLIS), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Bar.MIN_1_INTERVAL, 11145345);


    bolt.handle(md.getFields());

    ArgumentCaptor<Collection> emit = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(bolt, Mockito.atLeast(10)).publish(emit.capture());
    Mockito.verify(bolt, Mockito.atMost(10)).publish(emit.capture());

    Set<Field<Number>> args = new HashSet<>();

    emit.getAllValues().stream().forEach(c -> {
      args.addAll(c);
    });

    Assert.assertEquals(args.size(), 20);

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.ABS_CHANGE.field() + "-p2",
        BigDecimal.valueOf(11145345 - 12335253).setScale(4, BigDecimal.ROUND_HALF_UP));

    BaseField<BigDecimal> f2 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.PCT_CHANGE.field() + "-p2",
        BigDecimal.valueOf(11145345 - 12335253).divide(BigDecimal.valueOf(12335253), 4, BigDecimal.ROUND_HALF_UP));

    for (Field<Number> f : args) {
      System.out.println(f);
    }

    System.out.println("asserts:");
    System.out.println(f1);
    System.out.println(f2);

    Assert.assertTrue(args.contains(f1));
    Assert.assertTrue(args.contains(f2));

  }

  @Test
  public void testCompute10DiscreteChange() throws Exception {

    SymbolConfig config = new SymbolConfig();
    config.symbol.add("*");
    config.interval.add("*");
    config.periods.add("10");
    config.periods.add("15");

    bolt.getConfig(stream).put(BaseProcessor.TRANSFORMER_CONFIG_KEY,
        new TransformerConfig(Sets.newHashSet(config)));

    // Prime cache with prior tuple's fields
    List<Bar> cs = TestDataFactory.buildCandles(20, "GOOG", stream, new BigDecimal(10.19));

    cs.stream().forEach(c -> {
      bolt.handle(c.getFields());
    });

    // Validate results
    ArgumentCaptor<Collection> emit = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(bolt, Mockito.atLeast(11 * 5 + 6 * 5)).publish(emit.capture());
    Mockito.verify(bolt, Mockito.atMost(11 * 5 + 6 * 5)).publish(emit.capture());

  }



}
