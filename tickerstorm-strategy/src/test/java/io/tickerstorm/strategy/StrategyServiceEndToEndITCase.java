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

package io.tickerstorm.strategy;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.Session;
import io.tickerstorm.common.SessionFactory;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Trigger;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.test.TestDataFactory;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {StrategyServiceApplication.class, IntegrationTestContext.class})
public class StrategyServiceEndToEndITCase {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(StrategyServiceEndToEndITCase.class);

  @Autowired
  protected SessionFactory factory;

  protected Session session;

  @Qualifier("RealtimeMarketDataBusTest")
  @Autowired
  private EventBus realtimeBus;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  @Qualifier("TestNotificationBus")
  @Autowired
  protected EventBus notificationBus;

  private final ModelDataListener listener = new ModelDataListener();

  private String stream = "StrategyServiceEndToEndITCase";

  private final AtomicInteger counter = new AtomicInteger(0);

  long start;
  long time;


  @Before
  public void init() throws Exception {

    modelDataBus.register(listener);

    session = factory.newSession();
    session.configure(new DefaultResourceLoader().getResource("classpath:yml/modeldataendtoenditcase.yml").getInputStream(), stream);

    Trigger delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MARKET_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);

    delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MODEL_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);
    session.start();

    Thread.sleep(5000);
  }

  @Test
  public void testStrategyServiceEndToEnd() throws Exception {


    TestDataFactory.buildCandles(100, "goog", session.stream(), BigDecimal.ONE).stream().forEach(c -> {
      realtimeBus.post(c);
    });

    start = System.currentTimeMillis();
    while (counter.get() < 11950) {
      Thread.sleep(500);
      time = System.currentTimeMillis() - start;
    }

    Assert.assertEquals(11950, counter.get());

    if ((time / 1000) > 3) {
      Assert.fail("Test slowed down below 3s. Took " + time);
    }

  }

  @After
  public void cleanup() {
    logger.info("Model data count: " + counter.get() + " FINAL");
    logger.info("Strategy took " + ((System.currentTimeMillis() - start) / 1000) + "s FINAL");
  }

  private class ModelDataListener {

    @Subscribe
    public void onMessage(Field<?> f) {
      if ((counter.incrementAndGet() % 1000) == 0) {
        logger.info("Model data count: " + counter.get());
        logger.info("Strategy took " + ((System.currentTimeMillis() - start) / 1000) + "s");
      }
    }

    @Subscribe
    public void onMessage(Collection<Field<?>> f) {

      if ((counter.addAndGet(f.size()) % 10) == 0) {
        logger.info("Model data count: " + counter.get());
        logger.info("Strategy took " + ((System.currentTimeMillis() - start) / 1000) + "s");
      }
    }

  }

}
