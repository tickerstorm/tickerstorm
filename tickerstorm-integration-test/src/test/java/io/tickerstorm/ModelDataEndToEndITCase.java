package io.tickerstorm;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.data.dao.ModelDataDto;
import net.engio.mbassy.bus.MBassador;

@ContextConfiguration(classes = {IntegrationTestContext.class})
public class ModelDataEndToEndITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("brokerfeed")
  @Autowired
  private MBassador<MarketData> brokderFeed;

  @Autowired
  private CassandraOperations session;

  @Autowired
  private ModelDataDao modelDao;

  @Autowired
  private MarketDataDao dataDao;

  @BeforeClass
  public void waitFor() throws Exception {
    // let everything start up.
    //session.getSession().execute("TRUNCATE marketdata");
    //session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(5000);
  }

  @Test
  public void sendCanleEndToEnd() throws Exception {

    Candle c = new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    c.setStream("default");
    brokderFeed.publish(c);

    Thread.sleep(100000);

    long count = dataDao.count();
    assertEquals(count, 1);

    count = modelDao.count();
    assertEquals(count, 1);

    ModelDataDto dto = modelDao.findAll().iterator().next();

  }


}
