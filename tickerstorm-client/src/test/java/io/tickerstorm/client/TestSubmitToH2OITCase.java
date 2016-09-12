package io.tickerstorm.client;

import java.io.File;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Session;
import io.tickerstorm.common.entity.SessionFactory;
import net.engio.mbassy.bus.MBassador;

@ContextConfiguration(classes = {TestBacktestRunnerClientContext.class})
public class TestSubmitToH2OITCase extends AbstractTestNGSpringContextTests {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notifications;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private MBassador<Serializable> commandBus;

  @Autowired
  private SessionFactory factory;

  @Autowired
  private BacktestRunnerClient client;

  private static final String path = "/tmp/MarketDataFile-2015-10-17T04:19:43.322Z.csv";

  private Session session;

  @BeforeMethod
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File("/tmp/"));
    session = factory.newSession();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(path));
    session.end();
  }

  @Test
  public void testSubmitCSVToH2OOnEvent() throws Exception {

    Files.copy(new File("./src/test/resources/data/MarketDataFile-2015-10-17T04:19:43.322Z.csv"), new File(path));

    ExportModelDataToCSV export = new ExportModelDataToCSV(session.stream);
    export.markers.add(ExportModelDataToCSV.EXPORT_TO_CSV_MARKER);
    export.config.put(ExportModelDataToCSV.FILE_LOCATION, path);
    commandBus.publish(export);

  }

}
