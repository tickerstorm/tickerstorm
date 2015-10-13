package io.tickerstorm.data.query;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import io.tickerstorm.data.TestMarketDataServiceConfig;
import io.tickerstorm.data.converter.DataQueryClient;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class BaseDataQueryITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  protected DataQueryClient client;

  @Qualifier("historical")
  @Autowired
  protected MBassador<MarketData> bus;

  @Autowired
  protected MarketDataDao dao;

  protected Object verifier = null;

  @Autowired
  private CassandraOperations session;

  protected AtomicLong count = new AtomicLong(0);

  @BeforeMethod
  public void setup() throws Exception {
    session.getSession().execute("TRUNCATE marketdata");
    bus.subscribe(verifier);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    bus.unsubscribe(verifier);
    session.getSession().execute("TRUNCATE marketdata");
    count.set(0);
  }


}
