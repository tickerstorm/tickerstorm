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

import io.tickerstorm.common.command.BaseCompletionTracker;
import io.tickerstorm.common.command.CompletionTracker;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Notification;
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

    new BaseCompletionTracker(CompletionTracker.ModelData.isSaved(session.stream()), CompletionTracker.ModelData.isSaved(session.stream()),
        notificationsBus, 2000L, null, () -> {

          exportCommend = new ExportModelDataToCSV(session.stream());
          exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
          exportCommend.markers.add(Markers.LOCATION.toString());
          exportCommend.config.put(Markers.LOCATION.toString(), location);
          session.execute(exportCommend);

        });

    new BaseCompletionTracker(CompletionTracker.Ingest.someIngestStarted, null, CompletionTracker.Ingest.someIngestFinished,
        notificationsBus, 2000L, () -> {

          HistoricalFeedQuery query = new HistoricalFeedQuery(stream, "Google", new String[] {"TOL"});
          query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
          query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
          query.periods.add(Candle.MIN_1_INTERVAL);
          query.zone = ZoneOffset.ofHours(-7);
          session.execute(query);

        }, () -> {
          Assert.fail();
        });

    new BaseCompletionTracker(CompletionTracker.ModelData.Export.someCsvExportStarted, null,
        CompletionTracker.ModelData.Export.someCsvExportFinished, notificationsBus, 2000L, () -> {

          file_saved = true;

        }, () -> {
          Assert.fail();
        });

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
