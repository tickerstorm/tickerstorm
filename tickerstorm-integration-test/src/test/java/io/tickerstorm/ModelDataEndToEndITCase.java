package io.tickerstorm;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Session;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.data.dao.ModelDataDto;
import net.engio.mbassy.bus.MBassador;

@ContextConfiguration(classes = {IntegrationTestContext.class})
public class ModelDataEndToEndITCase extends AbstractTestNGSpringContextTests {

  private static final Logger logger = LoggerFactory.getLogger(ModelDataEndToEndITCase.class);

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private MBassador<MarketData> brokderFeed;

  @Autowired
  private CassandraOperations session;

  @Autowired
  private ModelDataDao modelDao;

  @Autowired
  private MarketDataDao dataDao;

  @Autowired
  private SessionFactory sFactory;

  private ModelDataDto dto;
  private Session s;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationBus;

  @BeforeMethod
  public void init() throws Exception {

    notificationBus.subscribe(this);

    // let everything start up.
    session.getSession().execute("TRUNCATE marketdata");
    session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(5000);

    s = sFactory.newSession();
    s.start();

  }

  @Test
  public void verifyModelDataStored() throws Exception {

    Candle c = new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    c.setStream(s.id);
    brokderFeed.publish(c);
    Thread.sleep(8000);

    long count = dataDao.count();
    assertEquals(count, 1);

    count = modelDao.count();
    assertEquals(count, 1);

    dto = modelDao.findAll().iterator().next();

    assertNotNull(dto);

    Map<String, Object> fields = dto.fromRow();
    
    assertTrue(fields.containsKey("simple-statistics"));
    assertTrue(fields.containsKey(Field.Name.MARKETDATA.field()));
    assertTrue(fields.containsKey(Field.Name.STREAM.field()));
    
    Collection<Field<?>> stats = (Collection<Field<?>>) fields.get("simple-statistics");
    stats.addAll(((MarketData)fields.get(Field.Name.MARKETDATA.field())).getFields());
    
    assertNotNull(stats);
    assertTrue(stats.size() > 4);

    for (Field<?> f : stats) {
      System.out.println(f.getName() + ": " + f.getValue());

    }

  }

  @AfterMethod
  public void end() {
    s.end();
  }

}
