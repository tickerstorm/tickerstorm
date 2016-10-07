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
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.command.CompletionTracker;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.ModelDataQuery;
import io.tickerstorm.common.command.Notification;
import io.tickerstorm.common.command.Trigger;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.BaseIntegrationTest;
import io.tickerstorm.data.IntegrationTestContext;

@DirtiesContext
@ContextConfiguration(classes = {IntegrationTestContext.class})
public class ModelDataEndToEndITCase extends BaseIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(ModelDataEndToEndITCase.class);

  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Autowired
  private EventBus brokderFeed;

  @Override
  public void onMarketDataServiceInitialized() throws Exception {
    session = factory.newSession();
    session.configure(new DefaultResourceLoader().getResource("classpath:yml/modeldataendtoenditcase.yml").getInputStream());

    Trigger delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MARKET_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);

    delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MODEL_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);

    Thread.sleep(5000);
    session.start();
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

    Candle c =
        new Candle("goog", session.stream(), Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    brokderFeed.post(c);
    Thread.sleep(10000);

    Assert.assertTrue(triggeredMarket.get());
    Assert.assertTrue(triggeredModel.get());

    ModelDataQuery q = new ModelDataQuery(session.stream());
    VerifyRetroModelQueryEnded handler3 = new VerifyRetroModelQueryEnded(triggeredRetro, q);
    notificationBus.register(handler3);
    session.execute(q);

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
      if (CompletionTracker.MarketData.isSaved(session.stream()).test(marker)) {
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
      if (q.isDone().test(marker)) {
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

      if (CompletionTracker.ModelData.isSaved(session.stream()).test(marker)) {

        Assert.assertTrue(marker.expect > 0);
        Thread.sleep(2000);
        result.set(true);
      }
    }
  }
}
