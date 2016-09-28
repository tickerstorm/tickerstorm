package io.tickerstorm.data.export;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.HistoricalFeedQuery;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.IntegrationTestContext;
import io.tickerstorm.service.HeartBeat;

@Profile("modeldata_export")
@ContextConfiguration(classes = {IntegrationTestContext.class})
public class ModelDataExporterITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  private SessionFactory factory;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  private ExportModelDataToCSV exportCommend;

  private String stream = "ModelDataExporterITCase";

  private final String location = "/tmp/testfile.csv";

  private boolean data_service_up = false;
  private boolean file_ingested = false;
  private boolean file_saved = false;
  private boolean modeL_data_saved = false;

  private io.tickerstorm.common.entity.Session session;

  @BeforeClass
  public void init() {
    notificationBus.register(this);
  }

  @AfterClass
  public void cleanup() {
    notificationBus.unregister(this);
  }

  @BeforeMethod
  public void setup() throws Exception {
    session = factory.newSession(stream);
    session.start();
    Files.deleteIfExists(new File(location).toPath());
  }

  @AfterMethod
  public void clean() {
    session.end();
  }

  @Test
  public void testExportToCSVFile() throws Exception {

    int count = 0;
    while (!file_ingested) {
      count++;
      Thread.sleep(2000);

      if (count > 10) {
        Assert.fail();
      }
    }
    
    
    while (!modeL_data_saved) {
      count++;
      Thread.sleep(2000);

      if (count > 10) {
        Assert.fail();
      }
    }    

    while (!file_saved) {
      count++;
      Thread.sleep(2000);

      if (count > 10) {
        Assert.fail();
      }
    }

    Assert.assertTrue(new File(location).exists());

  }

  @Subscribe
  public void onNotification(Notification not) throws Exception {

    logger.info(not);

    if (not.is(Markers.MODEL_DATA.toString()) && not.is(Markers.SAVE.toString()) && not.is(Markers.SUCCESS.toString()) && !modeL_data_saved && not.stream.equalsIgnoreCase(stream)) {

      modeL_data_saved = true;
      exportCommend = new ExportModelDataToCSV(session.stream());
      exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
      exportCommend.config.put(ExportModelDataToCSV.FILE_LOCATION, location);
      commandBus.post(exportCommend);

    } else if (not.is(Markers.FILE.toString()) && not.is(Markers.INGEST.toString()) && not.is(Markers.FAILED.toString())) {

      Assert.fail();

    } else if (not.is(Markers.FILE.toString()) && not.is(Markers.INGEST.toString()) && not.is(Markers.SUCCESS.toString())
        && !file_ingested && not.stream.equalsIgnoreCase("Google")) {

      file_ingested = true;

      HistoricalFeedQuery query = new HistoricalFeedQuery(stream, "Google", new String[] {"TOL"});
      query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
      query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
      query.periods.add(Candle.MIN_1_INTERVAL);
      query.zone = ZoneOffset.ofHours(-7);
      commandBus.post(query);

    } else if (not.is(Markers.FILE.toString()) && not.is(Markers.SAVE.toString()) && not.is(Markers.SUCCESS.toString()) && not.id.equals(exportCommend.id) && !file_saved) {
      file_saved = true;
    }
  }

  @Subscribe
  public void onHeartBeat(HeartBeat beat) throws Exception {

    logger.info(beat);

    if (beat.service.equals("data-service") && !data_service_up) {
      data_service_up = true;
      TestDataFactory.storeGoogleData(Locations.FILE_DROP_LOCATION);
    }
  }

}
