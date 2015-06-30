package io.tickerstorm.feed;

import io.tickerstorm.entity.Candle;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

@DirtiesContext
@ContextConfiguration(classes = { io.tickerstorm.CommonConfig.class })
public class HistoricalDataFeedITCase extends AbstractTestNGSpringContextTests {

  @Qualifier("query")
  @Autowired
  private EventBus bus;

  @Autowired
  private HistoricalDataFeed feed;
    
  @BeforeTest
  public void dataSetup(){
    
  }

  @Test
  public void testSimpleCandleQuery() {

    HistoricalFeedQuery query = new HistoricalFeedQuery("ITB", "DHI", "LEN", "PHM", "TOL", "NVR", "HD", "LOW", "TPH", "RYL", "MTH");
    query.interval = new Interval(new DateTime().minusDays(10), new DateTime());
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = DateTimeZone.forID("EST");
    
    feed.onQuery(query);
  }

}
