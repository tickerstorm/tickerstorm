package io.tickerstorm;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
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
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Session;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.data.dao.ModelDataDto;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

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

    AtomicBoolean triggeredModel = new AtomicBoolean(false);
    AtomicBoolean triggeredMarket = new AtomicBoolean(false);
    VerifyModelDataStoredHandler handler1 = new VerifyModelDataStoredHandler(triggeredModel);
    VerifyMarketDataStoredHandler handler2 = new VerifyMarketDataStoredHandler(triggeredMarket);
    notificationBus.subscribe(handler1);
    notificationBus.subscribe(handler2);

    Candle c = new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    c.setStream(s.id);
    brokderFeed.publishAsync(c);
    Thread.sleep(8000);

    Assert.assertTrue(triggeredModel.get());
    Assert.assertTrue(triggeredMarket.get());

    notificationBus.unsubscribe(handler1);
    notificationBus.unsubscribe(handler2);
  }

  private class VerifyMarketDataStoredHandler {

    AtomicBoolean result;

    public VerifyMarketDataStoredHandler(AtomicBoolean result) {
      this.result = result;
    }

    @Handler
    public void onData(BaseMarker marker) throws Exception {
      if ("google".equals(marker.stream) && marker.markers.contains(Markers.MARKET_DATA_SAVED.toString())) {
        Assert.assertEquals(new Integer(1), marker.expect);
        result.set(true);
      }
    }
  }

  private class VerifyModelDataStoredHandler {

    AtomicBoolean result;

    public VerifyModelDataStoredHandler(AtomicBoolean result) {
      this.result = result;
    }

    @Handler
    public void onData(BaseMarker marker) throws Exception {

      if (s.id.equals(marker.stream) && marker.markers.contains(Markers.MODEL_DATA_SAVED.toString())) {

        Assert.assertEquals(new Integer(3), marker.expect);

        Thread.sleep(2000);

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
        stats.addAll(((MarketData) fields.get(Field.Name.MARKETDATA.field())).getFields());

        assertNotNull(stats);
        assertTrue(stats.size() > 4);

        for (Field<?> f : stats) {
          System.out.println(f.getName() + ": " + f.getValue());
        }

        result.set(true);
      }

    }

  }


  @AfterMethod
  public void end() {
    s.end();
  }

}