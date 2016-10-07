package io.tickerstorm.data.feed;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class ModelDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  private EventBus queryBus;

  @Autowired
  private CassandraOperations session;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modeldataBus;

  @BeforeMethod
  public void dataSetup() throws Exception {
    session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(2000);
  }

  @Test
  public void testRequestModelData() throws Exception {

  }


}
