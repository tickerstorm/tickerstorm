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

package io.tickerstorm.data;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.Session;
import io.tickerstorm.common.SessionFactory;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.service.HeartBeat;
import org.junit.After;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public abstract class BaseIntegrationTest {

  @Autowired
  protected SessionFactory factory;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  protected EventBus notificationBus;

  private boolean data_service_up = false;

  protected Session session;

  @org.junit.BeforeClass
  public void init() throws Exception {
    notificationBus.register(this);

    while (!data_service_up) {
      Thread.sleep(1000);
    }
  }

  @org.junit.AfterClass
  public void cleanup() {
    notificationBus.unregister(this);
  }

  @Subscribe
  public final void onHeartBeat(HeartBeat beat) throws Exception {
    // logger.info(beat);

    if (beat.service.equals("data-service") && !data_service_up) {
      data_service_up = true;
      onMarketDataServiceInitialized();
    }
  }

  @After
  public void end() {
    session.end();
  }

  public abstract void onMarketDataServiceInitialized() throws Exception;

}
