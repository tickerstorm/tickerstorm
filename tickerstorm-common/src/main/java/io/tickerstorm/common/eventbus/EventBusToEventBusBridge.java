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

package io.tickerstorm.common.eventbus;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventBusToEventBusBridge<T> {

  private static final Logger logger = LoggerFactory.getLogger(EventBusToEventBusBridge.class);

  private final EventBus source;
  private final List<EventBus> listeners = new ArrayList<EventBus>();

  public EventBusToEventBusBridge(EventBus source, EventBus[] listeners) {
    this.source = source;
    this.source.register(this);
    this.listeners.addAll(Lists.newArrayList(listeners));
  }

  public EventBusToEventBusBridge(EventBus source, EventBus listener) {
    this.source = source;
    this.source.register(this);
    this.listeners.add(listener);
  }

  public void addListener(EventBus bus) {
    synchronized (this.listeners) {
      this.listeners.add(bus);
    }
  }

  @Subscribe
  public void onMessage(T o) {
    int i = 1;
    synchronized (listeners) {
      for (EventBus l : listeners) {
        logger.debug(
            "Bridging message " + o.toString() + " from " + source.identifier() + " to " + l
                .identifier() + ". " + i + "/"
                + this.listeners.size());
        l.post(o);
        i++;
      }
    }
  }

  @PreDestroy
  public void destroy() {
    this.source.unregister(this);
  }

}
