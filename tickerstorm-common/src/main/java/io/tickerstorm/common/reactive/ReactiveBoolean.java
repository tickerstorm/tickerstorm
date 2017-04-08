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

package io.tickerstorm.common.reactive;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Created by kkarski on 4/7/17.
 */
public class ReactiveBoolean {

  private final AtomicBoolean value = new AtomicBoolean(false);
  private final EventBus bus;
  private Predicate<Notification> falseCondition;
  private Predicate<Notification> trueCondition;

  ReactiveBoolean(EventBus bus) {
    this.bus = bus;
  }

  public ReactiveBoolean init(boolean val) {
    value.set(val);
    return this;
  }

  public ReactiveBoolean falseOn(Predicate<Notification> falseCondition) {
    this.falseCondition = falseCondition;
    return this;
  }

  public ReactiveBoolean trueOn(Predicate<Notification> trueCondition) {
    this.trueCondition = trueCondition;
    return this;
  }

  @Subscribe
  void onEvent(Notification event) {

    if (falseCondition != null && falseCondition.test(event)) {
      this.value.set(false);
    } else if (falseCondition != null && !falseCondition.test(event) && trueCondition == null) {
      this.value.set(true);
    }

    if (trueCondition != null && trueCondition.test(event)) {
      this.value.set(true);
    } else if (trueCondition != null && !trueCondition.test(event) && falseCondition == null) {
      this.value.set(false);
    }
  }

  public ReactiveBoolean start() {
    this.bus.register(this);
    return this;
  }

  public ReactiveBoolean pause() {
    this.bus.unregister(this);
    return this;
  }

  public boolean value() {
    return this.value.get();
  }

  @Override
  public String toString() {
    return String.valueOf(value());
  }
}
