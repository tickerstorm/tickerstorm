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

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.Notification;
import net.engio.mbassy.bus.MBassador;

@ContextConfiguration(classes = {TestBacktestRunnerClientContext.class})
public class TestSubmitToH2OITCase extends AbstractTestNGSpringContextTests {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notifications;

  @Autowired
  private BacktestRunnerClient client;

  private static final String path = "/tmp/MarketDataFile-2015-10-17T04:19:43.322Z.csv";

  @BeforeMethod
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File("/tmp/"));
  }

  @AfterMethod
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(path));
  }

  @Test
  public void testSubmitCSVToH2OOnEvent() throws Exception {

    Files.copy(new File("./src/test/resources/data/MarketDataFile-2015-10-17T04:19:43.322Z.csv"), new File(path));

    Notification n = new Notification();
    n.markers.add(Markers.SESSION_END.toString());
    n.markers.add(Markers.CSV_CREATED.toString());
    n.properties.put("output.file.csv.path", path);
    notifications.publish(n);

  }

}
