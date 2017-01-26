/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.data.feed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import io.tickerstorm.common.command.HistoricalFeedQuery;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.TestMarketDataServiceConfig;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class HistoricalJmsDataFeedITCase implements MessageListener {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Autowired
  private JmsTemplate jmsTemplate;

  private final String stream = "HistoricalJmsDataFeedITCase".toLowerCase();

  @Value("${service.name:data-service}")
  private String SERVICE;

  @Qualifier("realtime")
  @Autowired
  private DefaultMessageListenerContainer container;

  @Autowired
  private CassandraOperations session;

  boolean verified = false;
  final AtomicInteger count = new AtomicInteger(0);
  final long expCount = 778;

  @Before
  public void dataSetup() throws Exception {
    session.getSession().execute("TRUNCATE marketdata");
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Google"));
    Files.copy(new File("./src/test/resources/data/Google/TOL.csv"), new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
    Thread.sleep(3000);
    verified = false;

    if (!container.isRunning()) {
      container.setMessageListener(this);
      container.start();
    }
  }

  @After
  public void cleanup() {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));

    if (container.isRunning()) {
      container.shutdown();
    }
  }

  @Test
  public void testSimpleCandleQueryOverJMS() throws Exception {

    long st = System.currentTimeMillis();

    HistoricalFeedQuery query = new HistoricalFeedQuery(stream, "google", "TOL");
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

    Thread.sleep(3000);
    assertEquals(count.get(), expCount);
    assertTrue(verified);

    System.out.print("Test time: " + (System.currentTimeMillis() - st));

  }

  @Override
  public void onMessage(Message m) {

    try {

      MarketData md = (MarketData) ((ObjectMessage) m).getObject();

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), stream);
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
