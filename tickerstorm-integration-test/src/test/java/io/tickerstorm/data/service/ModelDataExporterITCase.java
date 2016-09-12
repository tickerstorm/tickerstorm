package io.tickerstorm.data.service;

import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.IntegrationTestContext;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.common.test.TestDataFactory;

@ContextConfiguration(classes = {IntegrationTestContext.class, ModelDataExporterITCase.Config.class})
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
  public void setup() {
    io.tickerstorm.common.entity.Session session = factory.newSession("Google");
    session.start();

    exportCommend = new ExportModelDataToCSV(session.stream);
    exportCommend.modelQuery.from = Instant.now().minus(700, ChronoUnit.DAYS);
    exportCommend.config.put(ExportModelDataToCSV.FILE_LOCATION, location);
  }

  @Test
  public void testExportToCSVFile() throws Exception {

    TestDataFactory.storeGoogleData();
    Thread.sleep(40000);
    commandBus.post(exportCommend);

    org.testng.Assert.assertTrue(new File(location).exists());

  }

  @Configuration
  public static class Config {

    @Bean
    public EventBusToEventBusBridge<MarketData> buildBridge(@Qualifier(Destinations.HISTORICL_MARKETDATA_BUS) EventBus source,
        @Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus listener) {
      EventBusToEventBusBridge<MarketData> bridge = new EventBusToEventBusBridge<MarketData>(source, listener);
      return bridge;

    }
  }


}
