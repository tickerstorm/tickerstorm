package io.tickerstorm.common.collections;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.test.TestDataFactory;

public class TestSynchronizedIndexedTreeMap {


  private Bar c;
  private Random rand = new Random();

  @Test
  public void populate200Count() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(Field.SORT_BY_INSTANTS, 200);

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      Instant in = Instant.now();
      insts.add(in);
      Thread.sleep(5);
      c = new Bar("GOOG", "google", in, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.ZERO, "1m", 10000);
      Assert.assertNull(q.put(in, new BaseField<>(c.getEventId(), "test_field-p" + i, rand.nextInt())));
    }

    Assert.assertEquals(q.size(), 200);
    Assert.assertTrue(q.lastKey().isBefore(q.firstKey()));
    Assert.assertEquals(q.lastKey(), insts.get(100));
    Assert.assertEquals(q.firstKey(), insts.get(299));
    Assert.assertTrue(insts.get(0).isBefore(insts.get(299)));
    Assert.assertFalse(q.containsKey(insts.get(0)));
    long start = System.currentTimeMillis();
    Assert.assertEquals(q.get(199).getTimestamp(), insts.get(100));
    System.out.println("Took: " + (System.currentTimeMillis() - start) + "ms");

  }

  @Test
  public void getLatest2() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(Field.SORT_BY_INSTANTS, 200);

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Instant in = Instant.now();
      insts.add(in);
      Thread.sleep(5);
      c = new Bar("GOOG", "google", in, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.ZERO, "1m", 10000);
      Assert.assertNull(q.put(in, new BaseField<>(c.getEventId(), "test_field-p" + i, rand.nextInt())));
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

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(Field.SORT_BY_INSTANTS, 200);

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      insts.add(Instant.now());
      Thread.sleep(5);
    }

    ArrayList<Instant> ordered = new ArrayList<>(insts);
    Collections.shuffle(insts, new Random(System.nanoTime()));

    for (Instant i : insts) {
      c = new Bar("GOOG", "google", i, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.ZERO, "1m", 10000);
      q.put(i, new BaseField<Integer>(c.getEventId(), "test_field-p" + i, rand.nextInt()));
    }

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
  public void testMultiThreadOperations() throws Exception {

    final SynchronizedIndexedTreeMap<Field<Integer>> q = new SynchronizedIndexedTreeMap<Field<Integer>>(Field.SORT_BY_INSTANTS, 200);

    AtomicBoolean ran1 = new AtomicBoolean(false);
    AtomicBoolean ran3 = new AtomicBoolean(false);

    Callable<Object> task1 = () -> {

      TestDataFactory.buildCandles(500, "goog", "google", BigDecimal.ONE).stream().parallel().forEach(c -> {
        Field<Integer> f = new BaseField<>(c.getEventId(), "volume", c.volume);
        q.put(f.getTimestamp(), f);
        System.out.print(f.getTimestamp().toEpochMilli() + " - " + q.firstEntry().getValue() + "\n");
        ran1.set(true);
      });

      return new Object();
    };

    Callable<Object> task3 = () -> {
      for (int i = 0; i < 100; i++) {
        try {

          if (q.firstEntry() != null) {
          
            Assert.assertNotNull(q.get(q.firstEntry().getValue().getTimestamp(), 50));
            Assert.assertEquals(q.subList(q.firstEntry().getValue().getTimestamp(), 50).size(), 50);
            System.out.println("50");
            ran3.set(true);
          
          }else{
            Thread.sleep(100);
          }
          
        } catch (Exception e) {
          System.out.println(e);
        }
      }
      return new Object();
    };


    ExecutorService exec = Executors.newFixedThreadPool(2);
    List<Future<Object>> futures = exec.invokeAll(Lists.newArrayList(task1, task3));

    if (futures.get(0).get(20, TimeUnit.SECONDS) != null && futures.get(1).get(20, TimeUnit.SECONDS) != null) {

      Assert.assertEquals(q.size(), 200);
      Assert.assertTrue(ran1.get());
      Assert.assertTrue(ran3.get());
    }
  }

}
