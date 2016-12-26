package io.tickerstorm.common.collections;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import com.google.common.collect.Lists;

/**
 * Ordered TimeSeries of elements in natural order with newest first (index 0) and oldest last
 * (index n).
 * 
 */
@SuppressWarnings("serial")
public class SynchronizedIndexedTreeMap<T> extends TreeMap<Instant, T> implements Iterable<T> {

  private final AtomicReference<List<Instant>> index = new AtomicReference<List<Instant>>();
  private final Integer maxSize;
  private final AtomicBoolean full = new AtomicBoolean(false);

  public SynchronizedIndexedTreeMap(Comparator<Instant> comp, Integer size) {
    super(comp);
    this.maxSize = size + 1;
  }

  public SynchronizedIndexedTreeMap(Integer size) {
    this.maxSize = size + 1;
  }

  @Override
  public synchronized T put(Instant key, T value) {

    if (full.get()) {
      pollFirstEntry();
    }

    T t = super.put(key, value);
    full.set(size() == (maxSize - 1));
    this.index.set(new ArrayList<Instant>(this.keySet()));
    return t;

  }

  @Override
  public synchronized void putAll(Map<? extends Instant, ? extends T> map) {

    for (Map.Entry e : map.entrySet()) {
      if (full.get()) {
        pollFirstEntry();
      }
      super.put((Instant) e.getKey(), (T) e.getValue());
      full.set(size() == (maxSize - 1));
      this.index.set(new ArrayList<Instant>(this.keySet()));
    }


  }

  @Override
  public synchronized java.util.Map.Entry<Instant, T> pollLastEntry() {

    Map.Entry<Instant, T> p = super.pollFirstEntry();
    full.set(size() == (maxSize - 1));
    this.index.set(new ArrayList<Instant>(this.keySet()));
    return p;

  }

  @Override
  public T merge(Instant key, T value, BiFunction<? super T, ? super T, ? extends T> remappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T putIfAbsent(Instant key, T value) {
    throw new UnsupportedOperationException();
  }

  public synchronized T get(Integer i) {

    Instant key = this.index.get().get(i);
    return get(key);

  }

  public synchronized List<T> subList(Instant until, Integer periods) {

    int in = this.index.get().indexOf(until);

    if (in > -1 && this.index.get().size() >= (in + periods)) {
      Instant inst = this.index.get().get(in + periods - 1);
      return Lists.newArrayList(this.subMap(until, true, inst, true).values());
    }

    return null;
  }

  public synchronized T get(Instant until, Integer periods) {

    T t = null;

    assert periods > 1 : "Number of periods should be more than 1";

    int in = this.index.get().indexOf(until);

    if (in > -1 && this.index.get().size() >= (in + periods)) {
      Instant inst = this.index.get().get(in + periods - 1);
      t = this.get(inst);
    }

    return t;
  }

  @Override
  public synchronized java.util.Map.Entry<Instant, T> pollFirstEntry() {

    Map.Entry<Instant, T> m = super.pollLastEntry();
    full.set(size() == (maxSize - 1));
    this.index.set(new ArrayList<Instant>(this.keySet()));
    return m;

  }

  @Override
  public Iterator<T> iterator() {
    return this.values().iterator();
  }

}
