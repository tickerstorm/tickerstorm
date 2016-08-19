package io.tickerstorm;

import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.data.query.ModelDataQuery;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Session;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.data.dao.ModelDataDto;
import io.tickerstorm.strategy.processor.NumericChangeProcessor;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@ContextConfiguration(classes = {IntegrationTestContext.class})
@IntegrationTest
public class ModelDataEndToEndITCase extends AbstractTestNGSpringContextTests {

  private static final Logger logger = LoggerFactory.getLogger(ModelDataEndToEndITCase.class);

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

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private MBassador<MarketData> brokderFeed;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private MBassador<Serializable> notificationBus;

  @Qualifier(Destinations.HISTORICAL_DATA_QUERY_BUS)
  @Autowired
  private MBassador<DataFeedQuery> queryBus;

  @BeforeMethod
  public void init() throws Exception {

    notificationBus.subscribe(this);

    // let everything start up.
    session.getSession().execute("TRUNCATE marketdata");
    session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(5000);

    s = sFactory.newSession();
    s.config.put(NumericChangeProcessor.PERIODS_CONFIG_KEY, "2");
    s.start();

  }

  @Test
  public void verifyModelDataStored() throws Exception {

    AtomicBoolean triggeredModel = new AtomicBoolean(false);
    AtomicBoolean triggeredMarket = new AtomicBoolean(false);
    AtomicBoolean triggeredRetro = new AtomicBoolean(false);
    VerifyModelDataStoredHandler handler1 = new VerifyModelDataStoredHandler(triggeredModel);
    VerifyMarketDataStoredHandler handler2 = new VerifyMarketDataStoredHandler(triggeredMarket);

    notificationBus.subscribe(handler1);
    notificationBus.subscribe(handler2);


    Candle c = new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    c.setStream(s.stream);
    brokderFeed.publishAsync(c);
    Thread.sleep(8000);

    Assert.assertTrue(triggeredMarket.get());
    Assert.assertTrue(triggeredModel.get());

    ModelDataQuery q = new ModelDataQuery(s.stream);
    VerifyRetroModelQueryEnded handler3 = new VerifyRetroModelQueryEnded(triggeredRetro, q);
    notificationBus.subscribe(handler3);
    queryBus.publish(q);

    Thread.sleep(2000);
    Assert.assertTrue(triggeredRetro.get());

    notificationBus.unsubscribe(handler1);
    notificationBus.unsubscribe(handler2);
    notificationBus.unsubscribe(handler3);
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

  private class VerifyRetroModelQueryEnded {

    AtomicBoolean result;
    ModelDataQuery q;

    public VerifyRetroModelQueryEnded(AtomicBoolean result, ModelDataQuery q) {
      this.result = result;
      this.q = q;
    }

    @Handler(condition = "msg.markers.contains('query_end')")
    public void onData(BaseMarker marker) throws Exception {

      if (marker.id.equals(q.id) && marker.markers.contains(Markers.QUERY_END.toString())) {
        result.set(true);
      }
    }

  }

  private class VerifyModelDataStoredHandler {

    AtomicBoolean result;

    public VerifyModelDataStoredHandler(AtomicBoolean result) {
      this.result = result;
    }

    @Handler(condition = "msg.markers.contains('model_data_saved')")
    public void onData(BaseMarker marker) throws Exception {

      if (s.stream.equals(marker.stream) && marker.markers.contains(Markers.MODEL_DATA_SAVED.toString())) {

        Assert.assertTrue(marker.expect > 0);

        Thread.sleep(2000);

        long count = modelDao.count();
        assertTrue(count > 0);

        Set<String> fieldNames = new java.util.HashSet<>();
        for (ModelDataDto dto : modelDao.findAll()) {
          Map<String, Object> values = dto.fromRow();
          fieldNames.addAll(values.keySet());
        }

        Assert.assertTrue(fieldNames.size() > 0);

        result.set(true);
      }

    }

  }


  @AfterMethod
  public void end() {
    s.end();
  }

}
