package io.tickerstorm.data.feed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class HistoricalJmsDataFeedITCase extends AbstractTestNGSpringContextTests implements MessageListener {

  @Autowired
  private JmsTemplate jmsTemplate;

  @Autowired
  private DefaultMessageListenerContainer container;

  @Autowired
  private CassandraOperations session;

  boolean verified = false;
  AtomicInteger count = new AtomicInteger(0);
  long expCount = 778;

  @BeforeClass
  public void dataSetup() throws Exception {
    session.getSession().execute("TRUNCATE marketdata");
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
    Thread.sleep(3000);
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
  }

  @BeforeMethod
  public void setup() {
    container.setMessageListener(this);
    count = new AtomicInteger(0);
    verified = false;
  }

  @Test
  public void testSimpleCandleQueryOverJMS() throws Exception {

    long st = System.currentTimeMillis();

    HistoricalFeedQuery query = new HistoricalFeedQuery("google", "google", new String[] {"TOL"});
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 11, 0, 0);
    query.periods.add(Bar.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);

    jmsTemplate.send(Destinations.TOPIC_COMMANDS, new MessageCreator() {

      @Override
      public Message createMessage(Session session) throws JMSException {

        Message m = session.createObjectMessage(query);
        return m;
      }
    });

    Thread.sleep(5000);
    assertEquals(count.get(), expCount);
    assertTrue(verified);

    System.out.print("Test time: " + (System.currentTimeMillis() - st));

  }

  @Override
  public void onMessage(Message m) {

    try {

      MarketData md = (MarketData) ((ObjectMessage) m).getObject();

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), "google");
      assertNotNull(md.getTimestamp());

      if (Bar.class.isAssignableFrom(md.getClass())) {

        Bar c = (Bar) md;
        assertNotNull(c.close);
        assertTrue(c.close.longValue() > 0);
        assertNotNull(c.open);
        assertTrue(c.open.longValue() > 0);
        assertNotNull(c.low);
        assertTrue(c.low.longValue() > 0);
        assertNotNull(c.high);
        assertTrue(c.high.longValue() > 0);
        assertNotNull(c.volume);
        assertTrue(c.volume.longValue() > 0);
        assertEquals(c.interval, Bar.MIN_1_INTERVAL);
        verified = true;
        synchronized (count) {
          count.incrementAndGet();
        }
      }

    } catch (Exception e) {
      Throwables.propagate(e);
    }

  }



}
