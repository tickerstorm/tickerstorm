package io.tickerstorm.data.export;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.CompletionTracker;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.command.OnEventHandler;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.BaseIntegrationTest;
import io.tickerstorm.data.IntegrationTestContext;

@ContextConfiguration(classes = {IntegrationTestContext.class})
public class ModelDataExporterITCase extends BaseIntegrationTest {

  private ExportModelDataToCSV exportCommend;

  private String stream = "ModelDataExporterITCase";

  private final String location = "/tmp/testfile.csv";

  private boolean file_saved = false;


  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Override
  public void onMarketDataServiceInitialized() throws Exception {
    session = factory.newSession(stream);
    session.start();
    TestDataFactory.storeGoogleData(Locations.FILE_DROP_LOCATION);
  }

  @BeforeMethod
  public void setup() throws Exception {
    Files.deleteIfExists(new File(location).toPath());
  }

  @Test
  public void testExportToCSVFile() throws Exception {

    OnEventHandler.newHandler(session.getNotificationsBus()).startCountDownOn(CompletionTracker.ModelData.isSaved(session.stream()))
        .extendTimeoutOn(CompletionTracker.ModelData.isSaved(session.stream())).timeoutDelay(2000).whenTimedOut(() -> {

          exportCommend = new ExportModelDataToCSV(session.stream());
          exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
          exportCommend.markers.add(Markers.LOCATION.toString());
          exportCommend.config.put(Markers.LOCATION.toString(), location);
          session.execute(exportCommend);

        }).start();

    OnEventHandler.newHandler(session.getNotificationsBus()).startCountDownOn(CompletionTracker.Ingest.someIngestStarted)
        .completeWhen(CompletionTracker.Ingest.someIngestFinished).timeoutDelay(2000).whenComplete((n) -> {

          HistoricalFeedQuery query = new HistoricalFeedQuery(stream, "Google", new String[] {"TOL"});
          query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
          query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
          query.periods.add(Candle.MIN_1_INTERVAL);
          query.zone = ZoneOffset.ofHours(-7);
          session.execute(query);

        }).whenTimedOut(() -> {
          Assert.fail();
        }).start();

    OnEventHandler.newHandler(session.getNotificationsBus()).startCountDownOn(CompletionTracker.ModelData.Export.someCsvExportStarted)
        .completeWhen(CompletionTracker.ModelData.Export.someCsvExportFinished).timeoutDelay(2000).whenComplete((n) -> {

          file_saved = true;

        }).whenTimedOut(() -> {
          Assert.fail();
        }).start();

    while (!file_saved) {
      Thread.sleep(5000);
    }

    Assert.assertTrue(new File(location).exists());

  }

  @Subscribe
  public void onNotification(Notification not) throws Exception {

    logger.info(not);

  }


}
