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

package io.tickerstorm.data.export;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.CompletionTracker;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.common.reactive.Observer;
import io.tickerstorm.common.reactive.ReactiveBoolean;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.BaseIntegrationTest;
import io.tickerstorm.data.IntegrationTestContext;
import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {IntegrationTestContext.class})
public class ModelDataExporterITCase extends BaseIntegrationTest {

  private final static Logger logger = LoggerFactory.getLogger(ModelDataExporterITCase.class);
  private final String location = "/tmp/testfile.csv";
  private ExportModelDataToCSV exportCommend;
  private String stream = "ModelDataExporterITCase";
  private AtomicBoolean file_saved = new AtomicBoolean(false);

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private EventBus brokderFeed;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Override
  public void onMarketDataServiceInitialized() throws Exception {
    session = factory.newSession();
    session.configure(new DefaultResourceLoader().getResource("classpath:yml/modeldataendtoenditcase.yml").getInputStream(), stream);
    session.start();

    Thread.sleep(5000);
  }

  @Before
  public void setup() throws Exception {
    super.init();
    java.nio.file.Files.deleteIfExists(new File(location).toPath());
  }

  @Test
  public void testExportToCSVFile() throws Exception {

    exportCommend = new ExportModelDataToCSV(session.stream());
    exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
    exportCommend.markers.add(Markers.LOCATION.toString());
    exportCommend.config.put(Markers.LOCATION.toString(), location);

    final ReactiveBoolean fileSaved = Observer.observe(session.getNotificationsBus()).newBoolean().trueOn(exportCommend.isDone()).start();

    Observer.observe(session.getNotificationsBus(), "export to csv").startCountDownOn(CompletionTracker.ModelData.isSaved(session.stream()))
        .extendTimeoutOn(CompletionTracker.ModelData.isSaved(session.stream()), 2000).whenTimedOut(() -> {

      session.execute(exportCommend);

    }).start();

    Observer.observe(session.getNotificationsBus(), "query historical feed").startCountDownOn(CompletionTracker.Ingest.someIngestStarted)
        .completeWhen(CompletionTracker.Ingest.someIngestFinished).mustCompleteWithin(2000).whenComplete((n) -> {

      HistoricalFeedQuery query = new HistoricalFeedQuery(stream, "Google", "TOL");
      query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
      query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
      query.periods.add(Bar.MIN_1_INTERVAL);
      query.zone = ZoneOffset.ofHours(-7);
      session.execute(query);

    }).whenTimedOut(() -> {
      Assert.fail();
    }).start();

//    Observer.observe(session.getNotificationsBus(), "file saved").startCountDownOn(exportCommend.started())
//        .completeWhen(exportCommend.isDone()).mustCompleteWithin(2000).whenComplete((n) -> {
//
//      file_saved.set(true);
//
//    }).whenTimedOut(() -> {
//      Assert.fail();
//    }).start();

    TestDataFactory.buildCandles(100, "goog", session.stream(), BigDecimal.ONE).stream().forEach(c -> {
      brokderFeed.post(c);
    });

    while (!fileSaved.value()) {
      Thread.sleep(5000);
    }

    Assert.assertTrue(new File(location).exists());

  }

  @Subscribe
  public void onNotification(Notification not) throws Exception {

    logger.debug(not.toString());

  }


}
