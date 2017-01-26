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
import io.tickerstorm.common.command.CompletionTracker;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.command.OnEventHandler;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.BaseIntegrationTest;
import io.tickerstorm.data.IntegrationTestContext;
import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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

  private ExportModelDataToCSV exportCommend;

  private String stream = "ModelDataExporterITCase";

  private final String location = "/tmp/testfile.csv";

  private boolean file_saved = false;

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
    java.nio.file.Files.deleteIfExists(new File(location).toPath());
  }

  @Test
  public void testExportToCSVFile() throws Exception {

    OnEventHandler.newHandler(session.getNotificationsBus()).startCountDownOn(CompletionTracker.ModelData.isSaved(session.stream()))
        .extendTimeoutOn(CompletionTracker.ModelData.isSaved(session.stream()), 2000).whenTimedOut(() -> {

          exportCommend = new ExportModelDataToCSV(session.stream());
          exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
          exportCommend.markers.add(Markers.LOCATION.toString());
          exportCommend.config.put(Markers.LOCATION.toString(), location);
          session.execute(exportCommend);

        }).start();

    OnEventHandler.newHandler(session.getNotificationsBus()).startCountDownOn(CompletionTracker.Ingest.someIngestStarted)
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

    OnEventHandler.newHandler(session.getNotificationsBus()).startCountDownOn(CompletionTracker.ModelData.Export.someCsvExportStarted)
        .completeWhen(CompletionTracker.ModelData.Export.someCsvExportFinished).mustCompleteWithin(2000).whenComplete((n) -> {

          file_saved = true;

        }).whenTimedOut(() -> {
          Assert.fail();
        }).start();

    TestDataFactory.buildCandles(100, "goog", session.stream(), BigDecimal.ONE).stream().forEach(c -> {
      brokderFeed.post(c);
    });

    while (!file_saved) {
      Thread.sleep(5000);
    }

    Assert.assertTrue(new File(location).exists());

  }

  @Subscribe
  public void onNotification(Notification not) throws Exception {

    logger.info(not.toString());

  }


}
