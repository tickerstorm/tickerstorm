package io.tickerstorm.strategy;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.Session;
import io.tickerstorm.common.SessionFactory;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.command.Trigger;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.test.TestDataFactory;

@SpringBootTest(classes = {StrategyServiceApplication.class, IntegrationTestContext.class})
public class StrategyServiceEndToEndITCase extends AbstractTestNGSpringContextTests {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(StrategyServiceEndToEndITCase.class);
  
  @Autowired
  protected SessionFactory factory;

  protected Session session;

  @Qualifier("RealtimeMarketDataBusTest")
  @Autowired
  private EventBus realtimeBus;
  
  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  @Qualifier("TestNotificationBus")
  @Autowired
  protected EventBus notificationBus;
  
  private final ModelDataListener listener = new ModelDataListener();

  private String stream = "StrategyServiceEndToEndITCase";
  
  private final AtomicInteger counter = new AtomicInteger(0);
  
  long start;
  long time;


  @BeforeMethod
  public void init() throws Exception {
    
    modelDataBus.register(listener);
    
    session = factory.newSession();
    session.configure(new DefaultResourceLoader().getResource("classpath:yml/modeldataendtoenditcase.yml").getInputStream(), stream);

    Trigger delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MARKET_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);

    delete = new Trigger(session.stream(), "delete.data");
    delete.addMarker(Markers.MODEL_DATA.toString());
    delete.addMarker(Markers.DELETE.toString());
    session.execute(delete);
    session.start();
    
    Thread.sleep(5000);
  }

  @Test
  public void testStrategyServiceEndToEnd() throws Exception {

    start = System.currentTimeMillis();
    TestDataFactory.buildCandles(100, "goog", session.stream(), BigDecimal.ONE).stream().forEach(c -> {
      realtimeBus.post(c);
    });
    
    while(counter.get() < 11950){
      Thread.sleep(500);
      time = System.currentTimeMillis() - start;
    }
    
    Assert.assertEquals(11950, counter.get());
    
    if((time/1000) > 5){
      Assert.fail("Test slowed down below 31s. Took " + time);
    }
  
  }
  
  @AfterMethod
  public void cleanup(){
    logger.info("Model data count: " + counter.get() + " FINAL"); 
    logger.info("Strategy took " + ((System.currentTimeMillis() - start)/1000) + "s FINAL");
  }
  
  private class ModelDataListener {    
    
    @Subscribe
    public synchronized void onMessage(Field<?> f){
      int last = counter.getAndIncrement();
      if((last % 1000) == 0){
        logger.info("Model data count: " + last);        
        logger.info("Strategy took " + ((System.currentTimeMillis() - start)/1000) + "s");
      }      
    }
    
    @Subscribe
    public synchronized void onMessage(Collection<Field<?>> f){
      int last = counter.getAndAdd(f.size());
      if((last % 10) == 0){
        logger.info("Model data count: " + last);        
        logger.info("Strategy took " + ((System.currentTimeMillis() - start)/1000) + "s");
      }      
    }
    
  }

}
