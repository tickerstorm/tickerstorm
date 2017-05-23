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

package io.tickerstorm.data.service;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.ModelDataQuery;
import io.tickerstorm.common.command.Trigger;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.CompletionTracker;
import io.tickerstorm.common.reactive.Observer;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.BaseIntegrationTest;
import io.tickerstorm.data.IntegrationTestContext;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {IntegrationTestContext.class})
public class ModelDataEndToEndITCase extends BaseIntegrationTest {

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private EventBus brokderFeed;

  private String stream = "ModelDataEndToEndITCase";

  private ModelDataQuery q;

  @Override
  public void onMarketDataServiceInitialized() throws Exception {
    session = factory.newSession();
    session.configure(new DefaultResourceLoader().getResource("classpath:yml/modeldataendtoenditcase.yml").getInputStream(), stream);
    session.start();
  }

  @After
  @Override
  public void cleanup() throws Exception {
    super.cleanup();

    Trigger delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MARKET_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);

    delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MODEL_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);

    session.end();

    Thread.sleep(3000);
  }

  @Test
  public void verifyModelDataStored() throws Exception {

    AtomicBoolean triggeredModel = new AtomicBoolean(false);
    AtomicBoolean triggeredMarket = new AtomicBoolean(false);
    AtomicBoolean triggeredRetro = new AtomicBoolean(false);

    q = new ModelDataQuery(session.stream());
    q.from = Instant.now().minus(1, ChronoUnit.DAYS);
    q.until = Instant.now().plus(5, ChronoUnit.SECONDS);

    Observer.observe(session.getNotificationsBus(), "marketdata")
        .startCountDownOn(CompletionTracker.MarketData.isSaved(session.stream()))
        .extendTimeoutOn(CompletionTracker.MarketData.isSaved(session.stream()), 1000).whenTimedOut(() -> {

      triggeredMarket.set(true);

    }).start();

    Observer.observe(session.getNotificationsBus(), "modeldata")
        .startCountDownOn(CompletionTracker.ModelData.isSaved(session.stream()))
        .extendTimeoutOn(CompletionTracker.ModelData.isSaved(session.stream()), 1000).whenTimedOut(() -> {

      triggeredModel.set(true);
      session.execute(q);

    }).start();

    Observer.observe(session.getNotificationsBus(), "query")
        .startCountDownOn(q.started())
        .completeWhen(q.isDone())
        .mustCompleteWithin(5000)
        .whenComplete((n) -> {
          triggeredRetro.set(true);
        }).whenTimedOut(() -> {
      org.junit.Assert.fail();
    }).start();

    TestDataFactory.buildCandles(100, "goog", session.stream(), BigDecimal.ONE).stream().forEach(c -> {
      brokderFeed.post(c);
    });

    while (!triggeredRetro.get()) {
      Thread.sleep(2000);
    }

    Thread.sleep(20000);

    Assert.assertTrue(triggeredRetro.get());
    Assert.assertTrue(triggeredModel.get());
    Assert.assertTrue(triggeredMarket.get());
  }

}
