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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

/**
 * Created by kkarski on 5/29/17.
 */
public class EventAggregator {

  private final EventBus bus;
  private final CopyOnWriteArrayList<Notification> events = new CopyOnWriteArrayList<>();
  private Predicate<Notification> collectOn = (l) -> {
    return true;
  };
  private Predicate<List<Notification>> emitWhen = (l) -> {
    return true;
  };
  private Consumer action;
  private boolean clearOnEmit = false;
  private boolean terminateOnEmit = false;

  EventAggregator(EventBus bus) {
    this.bus = bus;
  }

  public EventAggregator stopOnEmit(boolean t) {
    this.terminateOnEmit = t;
    return this;
  }

  public EventAggregator resetOnEmit(boolean t) {
    this.clearOnEmit = t;
    return this;
  }

  public EventAggregator onAggregation(Consumer<List<Notification>> action) {
    this.action = action;
    return this;
  }

  public EventAggregator onAggregation(Consumer<List<Notification>> action, Predicate<List<Notification>> when) {
    this.emitWhen = when;
    this.action = action;
    return this;
  }

  public EventAggregator filter(Predicate<Notification> collectOn) {
    this.collectOn = collectOn;
    return this;
  }

  public boolean start() {
    boolean valid = validate();

    if (valid) {
      this.bus.register(this);
    }

    return valid;
  }

  public boolean stop() {
    try {
      this.bus.unregister(this);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    return true;
  }

  public int count() {
    return this.events.size();
  }

  private boolean validate() {
    return (this.bus != null && action != null);
  }

  @Subscribe
  public void onNotification(Notification not) {

    if ((collectOn != null && collectOn.test(not))) {
      boolean emit = true;
      synchronized (events) {
        events.add(not);

        if (emitWhen != null) {
          emit = emitWhen.test(events);
        }

        if (emit) {
          this.action.accept(Lists.newCopyOnWriteArrayList(events));
        }

        if (clearOnEmit && emit) {
          events.clear();
        }
      }

      if (terminateOnEmit && emit) {
        stop();
      }
    }
  }
}
