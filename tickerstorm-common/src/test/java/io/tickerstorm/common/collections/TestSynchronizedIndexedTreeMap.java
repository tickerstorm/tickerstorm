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

package io.tickerstorm.common.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSynchronizedIndexedTreeMap {

  private final static Logger logger = LoggerFactory.getLogger(TestSynchronizedIndexedTreeMap.class);

  private Bar c;
  private Random rand = new Random();

  public static boolean between(int i, int minValueInclusive, int maxValueInclusive) {
    return (i >= minValueInclusive && i <= maxValueInclusive);
  }

  @Test
  public void populate200Count() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(
        Field.SORT_BY_INSTANTS, 200);

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      Instant in = Instant.now();
      insts.add(in);
      Thread.sleep(5);
      c = new Bar("GOOG", "google", in, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN,
          BigDecimal.ZERO, "1m", 10000);
      Assert.assertNull(
          q.put(in, new BaseField<>(c.getEventId(), "test_field-p" + i, rand.nextInt())));
    }

    Assert.assertEquals(200, q.size());
    Assert.assertTrue(q.lastKey().isBefore(q.firstKey()));
    Assert.assertEquals(q.lastKey(), insts.get(100));
    Assert.assertEquals(q.firstKey(), insts.get(299));
    Assert.assertTrue(insts.get(0).isBefore(insts.get(299)));
    Assert.assertFalse(q.containsKey(insts.get(0)));
    long start = System.currentTimeMillis();
    Assert.assertEquals(q.get(199).getTimestamp(), insts.get(100));
    Assert.assertEquals(q.firstEntry().getKey(), q.firstKey());
    Assert.assertEquals(q.firstEntry().getValue().getTimestamp(), q.firstKey());
    Assert.assertEquals(q.firstEntry().getKey(), q.firstEntry().getValue().getTimestamp());
    logger.info("Took: " + (System.currentTimeMillis() - start) + "ms");

  }

  @Test
  public void getXPeriods() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(
        Field.SORT_BY_INSTANTS, 200);

    TestDataFactory.buildCandles(200, "goog", "google", BigDecimal.ONE).stream()
        .forEach(c -> {
          Field<Integer> f = new BaseField<>(c.getEventId(), "volume", c.volume);
          Field<?> fx = q.put(f.getTimestamp(), f);
        });

    Assert.assertEquals(200, q.size());
    Assert.assertNotNull(q.get(q.firstKey(), 50));
    Assert.assertNotNull(q.get(q.firstEntry().getKey(), 50));
    Assert.assertNotNull(q.get(q.firstEntry().getValue().getTimestamp(), 50));
    Assert.assertNull(q.get(q.lastKey(), 50));
    Assert.assertNull(q.get(q.lastEntry().getKey(), 50));
    Assert.assertNull(q.get(q.lastEntry().getValue().getTimestamp(), 50));
    Assert.assertEquals(q.firstEntry().getKey(), q.firstKey());
    Assert.assertEquals(q.firstEntry().getValue().getTimestamp(), q.firstKey());
    Assert.assertEquals(q.firstEntry().getKey(), q.firstEntry().getValue().getTimestamp());

  }

  @Test
  public void getLatest2() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(
        Field.SORT_BY_INSTANTS, 200);

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Instant in = Instant.now();
      insts.add(in);
      Thread.sleep(5);
      c = new Bar("GOOG", "google", in, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN,
          BigDecimal.ZERO, "1m", 10000);
      Assert.assertNull(
          q.put(in, new BaseField<>(c.getEventId(), "test_field-p" + i, rand.nextInt())));
    }

    Assert.assertEquals(q.size(), 3);
    Assert.assertTrue(q.lastKey().isBefore(q.firstKey()));
    Assert.assertEquals(q.lastKey(), insts.get(0));
    Assert.assertEquals(q.firstKey(), insts.get(2));
    Assert.assertTrue(insts.get(0).isBefore(insts.get(2)));
    Assert.assertTrue(q.containsKey(insts.get(0)));
    long start = System.currentTimeMillis();
    Assert.assertEquals(q.get(0).getTimestamp(), insts.get(2));
    Assert.assertEquals(q.get(2).getTimestamp(), insts.get(0));

    Assert.assertEquals(insts.get(1), q.get(insts.get(2), 2).getTimestamp());
    Assert.assertEquals(insts.get(0), q.get(insts.get(1), 2).getTimestamp());
    Assert.assertNull(q.get(insts.get(0), 2));

  }

  @Test
  public void testIndexOrdering() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(
        Field.SORT_BY_INSTANTS, 200);

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      insts.add(Instant.now());
      Thread.sleep(5);
    }

    ArrayList<Instant> ordered = new ArrayList<>(insts);
    Collections.shuffle(insts, new Random(System.nanoTime()));

    for (Instant i : insts) {
      c = new Bar("GOOG", "google", i, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN,
          BigDecimal.ZERO, "1m", 10000);
      Assert.assertNull(q.put(i, new BaseField<Integer>(c.getEventId(), "test_field-p" + i, rand.nextInt())));
    }

    Assert.assertEquals(Sets.newHashSet(insts).size(), 200);
    Assert.assertEquals(q.size(), Sets.newHashSet(insts).size());
    Assert.assertEquals(q.keySet(), Sets.newHashSet(insts));
    Assert.assertEquals(q.get(0).getTimestamp(), ordered.get(199));
    Assert.assertEquals(q.get(100).getTimestamp(), ordered.get(99));
    Assert.assertTrue(q.get(0).getTimestamp().isAfter(q.get(100).getTimestamp()));

    Field<Integer> until = q.get(99);
    List<Field<Integer>> fs = q.subList(until.getTimestamp(), 10);
    Assert.assertEquals(fs.size(), 10);
    Assert.assertTrue(fs.contains(until));
    Assert.assertEquals(fs.get(0), until);
    Assert.assertTrue(until.getTimestamp().isAfter(fs.get(9).getTimestamp()));

    Field<Integer> f = q.get(until.getTimestamp(), 10);
    Assert.assertNotNull(f);
    Assert.assertTrue(f.getTimestamp().isBefore(until.getTimestamp()));
    Assert.assertEquals(fs.get(9), f);

  }

  @Test
  public void testStoreSameElement() throws Exception {

    List<Bar> bar = TestDataFactory.buildCandles(1, "goog", "TestSynchronizedIndexedTreeMap", new BigDecimal("23.5123"));

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(
        Field.SORT_BY_INSTANTS, 200);

    for (int i = 0; i < 20; i++) {
      q.put(bar.get(0).getTimestamp(), new BaseField<Integer>(bar.get(0).getEventId(), "test_field-p" + i, rand.nextInt()));
    }

    Assert.assertEquals(q.size(), 1);
    Assert.assertEquals(q.lastEntry(), q.firstEntry());

    for (Field<Integer> f : q) {
      Assert.assertEquals(f, q.lastEntry().getValue());
      Assert.assertEquals(f, q.firstEntry().getValue());
    }
  }

  @Test
  public void testMultiThreadOperations() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(
        Field.SORT_BY_INSTANTS, 200);

    AtomicBoolean ran1 = new AtomicBoolean(false);
    AtomicBoolean ran2 = new AtomicBoolean(false);
    AtomicBoolean ran3 = new AtomicBoolean(false);
    final AtomicInteger count = new AtomicInteger(0);
    final int full = 200;

    Callable<Object> task1 = () -> {

      TestDataFactory.buildCandles(full + 50, "goog", "google", BigDecimal.ONE).stream().parallel()
          .forEach(c -> {
            Field<Integer> f = new BaseField<>(c.getEventId(), "volume", c.volume);
            Field<?> fx = q.put(f.getTimestamp(), f);

            if (fx != null) {
              Assert.assertEquals(fx, f);
            } else {
              count.incrementAndGet();
            }

            Entry<Instant, Field<Integer>> e = q.firstEntry();
            Assert.assertEquals(e.getKey(), e.getValue().getTimestamp());
            System.out.println(f.getTimestamp().toEpochMilli() + " - " + q.firstEntry().getValue() + "\n");

          });

      Assert.assertEquals("Q size isn't 200", q.size(), full);
      Assert.assertEquals("Q size isn't 200", q.keySet().size(), full);
      Assert.assertNotNull("Getting 50 periods returned null", q.get(q.firstKey(), 50));

      for (Entry<Instant, Field<Integer>> s : q.entrySet()) {
        Assert.assertEquals(s.getKey(), s.getValue().getTimestamp());
      }

      ran1.set(true);

      boolean looped = false;
      Instant t = null;
      for (Field<Integer> f : q) {

        if (t != null) {
          Assert.assertTrue(t.isAfter(f.getTimestamp()));
          Assert.assertEquals(60, Duration.between(t, f.getTimestamp()).abs().get(ChronoUnit.SECONDS));
          looped = true;
        }
        t = f.getTimestamp();
      }

      Assert.assertTrue(looped);

      return new Object();
    };

    Callable<Object> task2 = () -> {

      int i = count.get();
      int j = q.size();

      while (!ran1.get()) {

        if (j > 0) {
          System.out.println(i + ", " + j);
          Entry<Instant, Field<Integer>> e = q.firstEntry();
          Assert.assertEquals(e.getKey(), e.getValue().getTimestamp());
        }

//        Assert.assertTrue("Expected " + i + " and was " + j, i >= j);

        if (j >= full) {
          ran2.set(true);
          Assert.assertEquals("full size not 200", full, j);
          Assert.assertEquals("Keysize " + j + " != " + q.keySet().size(), j, q.keySet().size());
        }

        i = count.get();
        j = q.size();
      }

      return new Object();
    };

    Callable<Object> task3 = () -> {
      while (!ran1.get()) {

        if (q.size() > 50) {

          Assert.assertNotNull("Getting 50 periods returned null", q.get(q.firstKey(), 50));
          Assert.assertEquals("Getting 50 periods didn't return 50 periods", 50, q.subList(q.firstKey(), 50).size());
          ran3.set(true);

        }
      }
      return new Object();
    };

    ExecutorService exec = Executors.newFixedThreadPool(3);
    List<Future<Object>> futures = exec.invokeAll(Lists.newArrayList(task1, task2, task3));

    if (futures.get(0).get(20, TimeUnit.SECONDS) != null
        && futures.get(1).get(20, TimeUnit.SECONDS) != null
        && futures.get(2).get(20, TimeUnit.SECONDS) != null
        ) {

      Assert.assertTrue("ran1 didn't finish", ran1.get());
      Assert.assertTrue("ran2 didn't finish", ran2.get());
      Assert.assertTrue("ran3 didn't finish", ran3.get());
    }
  }
}
