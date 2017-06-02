/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other materials provided with
 * the distribution.
 *
 * - Neither the name of Tickerstorm or the names of its contributors may be used to endorse or
 * promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.common.reactive;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.command.Markers;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.error.Mark;

/**
 * Created by kkarski on 6/2/17.
 */
public class TestEventAggregator {

  private final String stream = "TestEventAggregator";
  private EventBus supplier = new EventBus();
  private EventAggregator aggregator;

  @Before
  public void init() throws Exception {

    aggregator = Observer.observe(this.supplier).newAggregator();

  }

  @After
  public void cleanup() throws Exception {
    aggregator.stop();
  }

  private void issueEvents() {
    for (int i = 0; i < 3; i++) {
      Notification n = new Notification(UUID.randomUUID().toString(), stream);
      n.getMarkers().add(Markers.START.toString());
      supplier.post(n);

      n = new Notification(UUID.randomUUID().toString(), stream);
      n.getMarkers().add(Markers.IN_PROGRESS.toString());
      supplier.post(n);

      n = new Notification(UUID.randomUUID().toString(), stream);
      n.getMarkers().add(Markers.END.toString());
      supplier.post(n);
    }
  }

  @Test
  public void testAggregateAllEvents() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);
    Assert.assertTrue(aggregator.onAggregation((l) -> {

      counter.incrementAndGet();

    }).start());

    issueEvents();

    Assert.assertEquals(9, counter.get());
    Assert.assertEquals(9, aggregator.count());

  }

  @Test
  public void testAggregateFilterEvents() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);
    Assert.assertTrue(aggregator.onAggregation((l) -> {

      counter.incrementAndGet();

    }).filter((n) -> {

      return n.getMarkers().contains(Markers.START.toString());

    }).start());

    issueEvents();

    Assert.assertEquals(3, counter.get());
    Assert.assertEquals(3, aggregator.count());

  }

  @Test
  public void testEmitOnceAbove8() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);
    Assert.assertTrue(aggregator.onAggregation((l) -> {

      counter.incrementAndGet();

    }, (l) -> {
      return (l.size() > 8);
    }).start());

    issueEvents();

    Assert.assertEquals(1, counter.get());
    Assert.assertEquals(9, aggregator.count());

  }

  @Test
  public void testResetAfter3Events() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);
    Assert.assertTrue(aggregator.onAggregation((l) -> {

      counter.incrementAndGet();

    }, (l) -> {
      return (l.size() > 2);
    }).resetOnEmit(true).start());

    issueEvents();

    Assert.assertEquals(3, counter.get());
    Assert.assertEquals(0, aggregator.count());

  }
  
  @Test
  public void testTerminteAfter3Events() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);
    Assert.assertTrue(aggregator.onAggregation((l) -> {

      counter.incrementAndGet();

    }, (l) -> {
      return (l.size() > 2);
    }).stopOnEmit(true).start());

    issueEvents();

    Assert.assertEquals(1, counter.get());
    Assert.assertEquals(3, aggregator.count());

  }
}
