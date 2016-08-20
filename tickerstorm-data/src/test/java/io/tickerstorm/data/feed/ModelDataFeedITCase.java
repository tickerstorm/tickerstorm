package io.tickerstorm.data.feed;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import net.engio.mbassy.bus.MBassador;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class ModelDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("notification")
  @Autowired
  private MBassador<Serializable> notificationsBus;

  @Qualifier("query")
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

  @Autowired
  private CassandraOperations session;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private AsyncEventBus modeldataBus;

  @BeforeMethod
  public void dataSetup() throws Exception {
    session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(2000);
  }

  @Test
  public void testRequestModelData() throws Exception {

  }


}
