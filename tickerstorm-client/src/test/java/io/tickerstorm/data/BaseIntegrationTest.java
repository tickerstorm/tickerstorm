package io.tickerstorm.data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.Session;
import io.tickerstorm.common.SessionFactory;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.service.HeartBeat;

public abstract class BaseIntegrationTest extends AbstractTestNGSpringContextTests {

  @Autowired
  protected SessionFactory factory;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  protected EventBus notificationBus;

  private boolean data_service_up = false;

  protected Session session;

  @BeforeClass
  public void init() throws Exception {
    notificationBus.register(this);

    while (!data_service_up) {
      Thread.sleep(1000);
    }
  }

  @AfterClass
  public void cleanup() {
    notificationBus.unregister(this);
  }

  @Subscribe
  public final void onHeartBeat(HeartBeat beat) throws Exception {
    //logger.info(beat);

    if (beat.service.equals("data-service") && !data_service_up) {
      data_service_up = true;
      onMarketDataServiceInitialized();
    }
  }

  @AfterMethod
  public void end() {
    session.end();
  }

  public abstract void onMarketDataServiceInitialized() throws Exception;

}
