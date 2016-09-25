package io.tickerstorm.data.export;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.common.test.TestDataFactory;
import io.tickerstorm.data.IntegrationTestContext;

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

  private final String location = "/tmp/testfile.csv";

  @BeforeMethod
  public void setup() throws Exception {
    io.tickerstorm.common.entity.Session session = factory.newSession("Google");
    session.start();

    Files.deleteIfExists(new File(location).toPath());

    exportCommend = new ExportModelDataToCSV(session.stream());
    exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
    exportCommend.config.put(ExportModelDataToCSV.FILE_LOCATION, location);
  }

  @Test
  public void testExportToCSVFile() throws Exception {

    TestDataFactory.storeGoogleData();
    Thread.sleep(3000);
    commandBus.post(exportCommend);

    Thread.sleep(10000);

    Assert.assertTrue(new File(location).exists());

  }


}
