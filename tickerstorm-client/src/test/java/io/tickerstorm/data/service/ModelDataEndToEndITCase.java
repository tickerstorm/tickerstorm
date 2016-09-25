package io.tickerstorm.data.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.DeleteData;
import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.query.ModelDataQuery;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.common.entity.Session;
import io.tickerstorm.common.entity.SessionFactory;
import io.tickerstorm.data.IntegrationTestContext;

@DirtiesContext
@ContextConfiguration(classes = {IntegrationTestContext.class})
public class ModelDataEndToEndITCase extends AbstractTestNGSpringContextTests {

  private static final Logger logger = LoggerFactory.getLogger(ModelDataEndToEndITCase.class);

  @Autowired
  private SessionFactory sFactory;

  private Session s;

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private EventBus brokderFeed;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandBus;

  @BeforeMethod
  public void init() throws Exception {

    notificationBus.register(this);

    s = sFactory.newSession();
    s.configure(new DefaultResourceLoader().getResource("classpath:yml/modeldataendtoenditcase.yml").getInputStream());

    DeleteData delete = new DeleteData(s.stream());
    delete.addMarker(Markers.MARKET_DATA.toString());
    commandBus.post(delete);

    delete = new DeleteData(s.stream());
    delete.addMarker(Markers.MODEL_DATA.toString());
    commandBus.post(delete);

    Thread.sleep(5000);
    s.start();

  }

  @Test
  public void verifyModelDataStored() throws Exception {

    AtomicBoolean triggeredModel = new AtomicBoolean(false);
    AtomicBoolean triggeredMarket = new AtomicBoolean(false);
    AtomicBoolean triggeredRetro = new AtomicBoolean(false);
    VerifyModelDataStoredHandler handler1 = new VerifyModelDataStoredHandler(triggeredModel);
    VerifyMarketDataStoredHandler handler2 = new VerifyMarketDataStoredHandler(triggeredMarket);

    notificationBus.register(handler1);
    notificationBus.register(handler2);

    Candle c = new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    brokderFeed.post(c);
    Thread.sleep(250000);

    Assert.assertTrue(triggeredMarket.get());
    Assert.assertTrue(triggeredModel.get());

    ModelDataQuery q = new ModelDataQuery(s.stream());
    VerifyRetroModelQueryEnded handler3 = new VerifyRetroModelQueryEnded(triggeredRetro, q);
    notificationBus.register(handler3);
    commandBus.post(q);

    Thread.sleep(2000);
    Assert.assertTrue(triggeredRetro.get());

    notificationBus.unregister(handler1);
    notificationBus.unregister(handler2);
    notificationBus.unregister(handler3);
  }

  private class VerifyMarketDataStoredHandler {

    AtomicBoolean result;

    public VerifyMarketDataStoredHandler(AtomicBoolean result) {
      this.result = result;
    }

    @Subscribe
    public void onData(Notification marker) throws Exception {
      if ("google".equals(marker.stream) && marker.markers.contains(Markers.MARKET_DATA.toString())
          && marker.markers.contains(Markers.SAVED.toString())) {
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

    @Subscribe
    public void onData(Notification marker) throws Exception {

      if (marker.id.equals(q.id) && marker.markers.contains(Markers.QUERY.toString()) && marker.markers.contains(Markers.END.toString())) {
        result.set(true);
      }
    }

  }

  private class VerifyModelDataStoredHandler {

    AtomicBoolean result;

    public VerifyModelDataStoredHandler(AtomicBoolean result) {
      this.result = result;
    }

    @Subscribe
    public void onData(Notification marker) throws Exception {

      if (s.stream().equals(marker.stream) && marker.markers.contains(Markers.MODEL_DATA.toString())
          && marker.markers.contains(Markers.SAVED.toString())) {

        Assert.assertTrue(marker.expect > 0);

        Thread.sleep(2000);

        result.set(true);
      }

    }

  }


  @AfterMethod
  public void end() {
    s.end();
  }

}
