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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * Ordered TimeSeries of elements in natural order with newest first (index 0) and oldest last
 * (index n). As new elements are added and the map fills up, the oldest element is removed to make room for younger values.
 * Values older than the oldest will be ignored when put.
 *
 * @Author Krzysztof Karski
 */
@SuppressWarnings("serial")
public class SynchronizedIndexedTreeMap<T> extends ConcurrentSkipListMap<Instant, T> implements Iterable<T> {

  private final AtomicReference<List<Instant>> index = new AtomicReference<List<Instant>>();
  private final Integer maxSize;
  private final AtomicInteger full = new AtomicInteger(0);

  public SynchronizedIndexedTreeMap(Comparator<Instant> comp, Integer size) {
    super(comp);
    this.maxSize = size;
  }

  public SynchronizedIndexedTreeMap(Integer size) {
    this.maxSize = size;
  }

  @Override
  public T put(Instant key, T value) {

    assert value != null;
    assert key != null;

    //ignore values older than the oldest when map is full
    if (full.get() >= maxSize && lastEntry().getKey().isAfter(key)) {
      return value;
    }

    synchronized (full) {

      T t = super.put(key, value);

      //If we get an old value, replacement happened and size doesn't change.
      if (t == null) {

        if (full.get() < maxSize) {

          full.incrementAndGet();

        } else if (full.get() >= maxSize) {

          super.pollLastEntry();//do not use public poll since it decrements

        }

        this.index.set(new ArrayList<Instant>(this.keySet()));
      }

      return t;
    }

  }

  @Override
  public void putAll(Map<? extends Instant, ? extends T> map) {
    for (Map.Entry e : map.entrySet()) {
      this.put((Instant) e.getKey(), (T) e.getValue());
    }
  }

  @Override
  public java.util.Map.Entry<Instant, T> pollLastEntry() {

    synchronized (full) {
      //Remove from reverse end
      Map.Entry<Instant, T> p = super.pollLastEntry();
      if (p != null) {
        full.decrementAndGet();
        this.index.set(new ArrayList<Instant>(this.keySet()));
      }

      return p;
    }

  }

  @Override
  public T merge(Instant key, T value, BiFunction<? super T, ? super T, ? extends T> remappingFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T putIfAbsent(Instant key, T value) {
    throw new UnsupportedOperationException();
  }

  public T get(Integer i) {

    synchronized (full) {
      Instant key = this.index.get().get(i);
      return super.get(key);
    }

  }

  public List<T> subList(Instant until, Integer periods) {

    ConcurrentSkipListMap<Instant, T> tm = new ConcurrentSkipListMap<Instant, T>(this);
    List<Instant> copy = new ArrayList<Instant>(tm.keySet());

    int in = copy.indexOf(until);

    if (in > -1 && copy.size() >= (in + periods)) {
      Instant inst = copy.get(in + (periods - 1));
      return Lists.newArrayList(tm.subMap(until, true, inst, true).values());
    }

    return null;

  }

  public T get(Instant from, Integer periods) {

    T t = null;

    assert periods > 1 : "Number of periods should be more than 1";

    ConcurrentSkipListMap<Instant, T> tm = new ConcurrentSkipListMap<Instant, T>(this);
    List<Instant> copy = new ArrayList<Instant>(tm.keySet());

    int in = copy.indexOf(from);

    if (in > -1 && copy.size() >= (in + periods)) {
      Instant inst = copy.get(in + (periods - 1));
      t = tm.get(inst);
    }

    return t;

  }

  @Override
  public java.util.Map.Entry<Instant, T> pollFirstEntry() {

    synchronized (full) {
      Map.Entry<Instant, T> m = super.pollFirstEntry();
      if (m != null) {
        full.decrementAndGet();
        this.index.set(new ArrayList<Instant>(this.keySet()));
      }
      return m;
    }

  }

  @Override
  public Iterator<T> iterator() {
    return this.values().iterator();
  }

  @Override
  public synchronized void clear() {
    synchronized (full) {
      full.set(0);
      this.index.get().clear();
      super.clear();
    }
  }

  @Override
  public int size() {
    return full.get();
  }
}
