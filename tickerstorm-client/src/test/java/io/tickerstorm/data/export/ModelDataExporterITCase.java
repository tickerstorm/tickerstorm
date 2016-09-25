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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.data.eventbus.Destinations;
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

  private final String location = "/tmp/testfile.csv";

  private boolean data_service_up = false;
  
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
    io.tickerstorm.common.entity.Session session = factory.newSession("Google");
    session.start();

    Files.deleteIfExists(new File(location).toPath());

    exportCommend = new ExportModelDataToCSV(session.stream());
    exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
    exportCommend.config.put(ExportModelDataToCSV.FILE_LOCATION, location);
  }

  @Test
  public void testExportToCSVFile() throws Exception {


  }

  @Subscribe
  public void onHeartBeat(HeartBeat beat) throws Exception {
        
    if(beat.service.equals("data-service") && !data_service_up){
      
      data_service_up = true;

      TestDataFactory.storeGoogleData(Locations.FILE_DROP_LOCATION);
      Thread.sleep(3000);
      commandBus.post(exportCommend);

      Thread.sleep(10000);
      Assert.assertTrue(new File(location).exists());

      
    }    
  }

}
