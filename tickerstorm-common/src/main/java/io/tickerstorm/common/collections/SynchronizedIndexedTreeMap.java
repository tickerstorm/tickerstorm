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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * Ordered TimeSeries of elements in natural order with newest first (index 0) and oldest last
 * (index n).
 * @Author Krzysztof Karski
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

  public synchronized T get(Instant from, Integer periods) {

    T t = null;

    assert periods > 1 : "Number of periods should be more than 1";

    int in = this.index.get().indexOf(from);

    if (in > -1 && this.index.get().size() >= (in + periods)) {
      Instant inst = this.index.get().get(in + (periods - 1));
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
