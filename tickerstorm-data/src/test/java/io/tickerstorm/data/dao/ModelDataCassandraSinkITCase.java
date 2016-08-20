package io.tickerstorm.data.dao;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.google.common.eventbus.AsyncEventBus;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.BaseMarker;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.data.TestMarketDataServiceConfig;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class ModelDataCassandraSinkITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  private CassandraOperations session;

  @Qualifier(Destinations.MODEL_DATA_BUS)
  @Autowired
  private AsyncEventBus modelDataBus;

  @Autowired
  private ModelDataDao dao;

  private final Instant instant = Instant.now();
  private final String symbol = "goog";
  private final String source = "google";
  private final String stream = "ModelDataCassandraSinkITCase";
  private Candle c;

  @org.testng.annotations.BeforeClass
  public void cleanup() throws Exception {
    session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(2000);

    c = new Candle(symbol, source, this.instant, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    c.setStream(stream);
  }

  @Test(priority = 1)
  public void testStoreModelData() throws Exception {

    Set<Field<?>> tuple = new HashSet<>();
    BaseField<Integer> df = new BaseField<Integer>(c.getEventId(), "ave", 100);
    BaseField<BigDecimal> cf = new BaseField<BigDecimal>(c.getEventId(), "sma", BigDecimal.TEN);

    tuple.add(df);
    tuple.add(cf);

    modelDataBus.post(c);
    modelDataBus.post(tuple);

    Thread.sleep(5000);

    long count = dao.count();

    assertEquals(count, 1);

    Iterable<ModelDataDto> result = dao.findAll();

    Set<ModelDataDto> dtos = new HashSet<>();
    for (ModelDataDto dto : result) {
      dtos.add(dto);
    }

    assertEquals(dtos.size(), 1);

    tuple.clear();
    tuple.addAll(c.getFields());
    tuple.add(df);
    tuple.add(cf);

    Assert.assertTrue(dtos.stream().allMatch(d -> {

      return (tuple.containsAll(d.asFields()));

    }));

    dtos.stream().forEach(d -> {

      Assert.assertNotNull(d.primarykey);
      Assert.assertNotNull(d.primarykey.date);
      Assert.assertNotNull(d.primarykey.timestamp);
      Assert.assertEquals(d.primarykey.timestamp, Date.from(c.timestamp));
      Assert.assertEquals(d.primarykey.stream, c.stream);

    });
  }

  @Test(priority = 2)
  public void testStoreSecondSetOfFieldsModelData() throws Exception {

    BaseField<Integer> df = new BaseField<Integer>(c.getEventId(), Field.Name.AVE.field() + "-p1", 102);
    BaseField<BigDecimal> max = new BaseField<BigDecimal>(c.getEventId(), Field.Name.MAX.field() + "-p1", new BigDecimal("11.2"));
    BaseField<BigDecimal> min = new BaseField<BigDecimal>(c.getEventId(), Field.Name.MIN.field() + "-p1", new BigDecimal("1.2"));
    BaseField<BigDecimal> std = new BaseField<BigDecimal>(c.getEventId(), Field.Name.STD.field() + "-p1", new BigDecimal("19.1"));
    BaseField<BigDecimal> sma = new BaseField<BigDecimal>(c.getEventId(), Field.Name.SMA.field() + "-p1", new BigDecimal("0.2"));

    modelDataBus.post(df);
    modelDataBus.post(max);
    modelDataBus.post(min);
    modelDataBus.post(std);
    modelDataBus.post(sma);

    Thread.sleep(5000);

    long count = dao.count();

    assertEquals(count, 1);

    Set<Field<?>> tuple = new HashSet<>();
    Set<ModelDataDto> dtos = new HashSet<>();
    Iterable<ModelDataDto> result = dao.findAll();

    for (ModelDataDto dto : result) {
      dtos.add(dto);
    }

    tuple.addAll(Sets.newHashSet(df, max, min, std, sma));
    tuple.addAll(c.getFields());
    tuple.add(new BaseField<Integer>(c.getEventId(), "ave", 100));
    tuple.add(new BaseField<BigDecimal>(c.getEventId(), "sma", BigDecimal.TEN));

    Assert.assertTrue(dtos.stream().allMatch(d -> {

      return (tuple.containsAll(d.asFields()));

    }));

    dtos.stream().forEach(d -> {

      Assert.assertNotNull(d.primarykey);
      Assert.assertNotNull(d.primarykey.date);
      Assert.assertNotNull(d.primarykey.timestamp);
      Assert.assertEquals(d.primarykey.timestamp, Date.from(c.timestamp));
      Assert.assertEquals(d.primarykey.stream, c.stream);

    });

  }

  @Test(priority = 3)
  public void testStoreMarker() throws Exception {

    session.getSession().execute("TRUNCATE modeldata");
    Thread.sleep(2000);

    BaseMarker marker = new BaseMarker(UUID.randomUUID().toString(), "Default");
    marker.addMarker(Markers.QUERY_START.toString());
    marker.expect = 100;

    modelDataBus.post(marker);
    modelDataBus.post("test model");

    Thread.sleep(5000);

    long count = dao.count();

    assertEquals(count, 0);
  }
}
