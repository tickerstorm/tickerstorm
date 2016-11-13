package io.tickerstorm.common.collections;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;

public class TestSynchronizedIndexedTreeMap {

  private SynchronizedIndexedTreeMap<Field<Integer>> q;
  private Bar c;
  private Random rand = new Random();

  @BeforeMethod
  public void init() {
    q = new SynchronizedIndexedTreeMap<Field<Integer>>(Field.SORT_BY_INSTANTS, 200);
  }

  @Test
  public void populate200Count() throws Exception {

    ArrayList<Instant> insts = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      Instant in = Instant.now();
      insts.add(in);
      Thread.sleep(5);
      c = new Bar("GOOG", "google", in, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.ZERO, "1m", 10000);
      Assert.assertNull(q.put(in, new BaseField<>(c.getEventId(), "test_field-p" + i, rand.nextInt())));
    }

    Assert.assertEquals(q.size(), 201);
    Assert.assertTrue(q.lastKey().isBefore(q.firstKey()));
    Assert.assertEquals(q.lastKey(), insts.get(99));
    Assert.assertEquals(q.firstKey(), insts.get(299));
    Assert.assertTrue(insts.get(0).isBefore(insts.get(299)));
    Assert.assertFalse(q.containsKey(insts.get(0)));
    long start = System.currentTimeMillis();
    Assert.assertEquals(q.get(199).getTimestamp(), insts.get(100));
    System.out.println("Took: " + (System.currentTimeMillis() - start) + "ms");

  }

  @Test
  public void getLatest2() throws Exception {

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
    Assert.assertEquals(fs.size(), 11);
    Assert.assertTrue(fs.contains(until));
    Assert.assertEquals(fs.get(0), until);
    Assert.assertTrue(until.getTimestamp().isAfter(fs.get(10).getTimestamp()));
            
    Field<Integer> f = q.get(until.getTimestamp(), 10);
    Assert.assertNotNull(f);
    Assert.assertTrue(f.getTimestamp().isBefore(until.getTimestamp()));
    Assert.assertEquals(fs.get(9), f);    
    
  }
}
