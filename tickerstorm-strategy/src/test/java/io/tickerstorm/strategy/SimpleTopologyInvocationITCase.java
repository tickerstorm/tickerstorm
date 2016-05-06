package io.tickerstorm.strategy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;

@ContextConfiguration(classes = {StrategyServiceTestApplication.class})
public class SimpleTopologyInvocationITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  @Qualifier("modelData")
  @Autowired
  private MBassador<Map<String, Object>> modelData;

  private boolean verified1 = false;
  
  @BeforeClass
  public void waitFor() throws Exception {
    //let everything start up.
    Thread.sleep(5000);
  }

  @Test
  public void sendACandle() throws Exception {

    Object listener = new SendACandleVerifier();

    modelData.subscribe(listener);

    Candle c = new Candle("MSFT", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.ONE, "1m", 100000);
    realtimeBus.publish(c);

    Thread.sleep(10000);

    Assert.assertTrue(verified1);

    modelData.unsubscribe(listener);
  }

  @Listener()
  public class SendACandleVerifier {

    @Handler
    public void onModelData(Map<String, Object> modelData) {

      verified1 = true;

    }
  }

}
