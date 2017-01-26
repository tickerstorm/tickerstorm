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

package io.tickerstorm.data.query;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.converter.DataQueryClient;
import io.tickerstorm.data.dao.MarketDataDao;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;

public abstract class BaseDataQueryITCase {

  @Autowired
  protected DataQueryClient client;

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  protected EventBus bus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  protected EventBus notificationsBus;

  @Autowired
  protected MarketDataDao dao;

  protected Object verifier = null;

  @Autowired
  private CassandraOperations session;

  protected AtomicLong count = new AtomicLong(0);

  @Before
  public void setup() throws Exception {
    session.getSession().execute("TRUNCATE marketdata");
    bus.register(verifier);
  }

  @After
  public void tearDown() throws Exception {
    bus.unregister(verifier);
    session.getSession().execute("TRUNCATE marketdata");
    session.getSession().execute("TRUNCATE modeldata");
    count.set(0);
    Thread.sleep(2000);
  }


}
