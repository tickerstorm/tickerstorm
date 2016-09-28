package io.tickerstorm.data.dao;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class CassandraPerformanceITCase extends AbstractTestNGSpringContextTests {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationsBus;

  @Autowired
  private ModelDataDao dao;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private EventBus modelDataBus;

  private final List<ModelDataDto> dtos = new ArrayList<>();
  private final List<Candle> cs = new ArrayList<>();
  private final String stream = UUID.randomUUID().toString();

  @BeforeMethod
  public void clean() throws Exception {
    dao.deleteByStream(stream);
    Thread.sleep(2000);
  }

  @BeforeTest
  public void init() throws Exception {

    long time = System.currentTimeMillis();
    for (int j = 0; j < 2000; j++) {
      Candle c = new Candle("Goog", stream, Instant.now().plus(1, ChronoUnit.MINUTES), new BigDecimal(Math.random()),
          new BigDecimal(Math.random()), new BigDecimal(Math.random()), new BigDecimal(Math.random()), "1m",
          Double.valueOf(Math.random()).intValue());

      for (int i = 0; i < 3; i++) {
        for (Field<?> f : c.getFields()) {
          c.getFields().add(new BaseField<BigDecimal>(f, "test-field-p" + i, new BigDecimal(Math.random())));
        }
      }

      cs.add(c);
      ModelDataDto dto = ModelDataDto.convert(c);
      dtos.add(dto);

    }
    System.out.println("Converting market data of size " + dtos.size() + " took " + (System.currentTimeMillis() - time) + "ms");

  }

  @Test
  public void testPureCassandraStorage() {

    long time = System.currentTimeMillis();
    dao.ingest(dtos);
    System.out.println("Pure cassandra storage of " + dtos.size() + " took " + (System.currentTimeMillis() - time) + "ms");

  }

  @Test
  public void testStoreToCassandraViaModelDataBus() throws Exception {

    final AtomicInteger i = new AtomicInteger(0);
    long time = System.currentTimeMillis();
    notificationsBus.register(new CassandraStorageEventListener(time));
    cs.forEach(c -> {
      c.getFields().forEach(f -> {
        i.addAndGet(1);
        modelDataBus.post(f);
      });
    });

    System.out.println("Dispatching " + i + " fields took " + (System.currentTimeMillis() - time) + "ms");
    Thread.sleep(8000);

  }

  private class CassandraStorageEventListener {

    long time = 0L;
    final AtomicInteger count = new AtomicInteger(0);

    public CassandraStorageEventListener(long start) {
      this.time = start;
    }

    @Subscribe
    public void onNotification(Serializable s) {

      if (s instanceof Notification) {

        Notification n = (Notification) s;

        synchronized (count) {
          if (n.markers.contains(Markers.MODEL_DATA.toString()) && n.markers.contains(Markers.SAVE.toString())) {
            count.addAndGet(n.expect);
          }
        }

        System.out.println("Marker " + count.get());

        if (count.get() == 18000) {
          System.out.println("Persisting " + count.get() + " fields took " + n.eventTime.minus(time, ChronoUnit.MILLIS) + "ms");
        }
      }

    }

  }

}
