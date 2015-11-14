package io.tickerstorm.data.dao;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.CategoricalField;
import io.tickerstorm.common.entity.ContinousField;
import io.tickerstorm.common.entity.DiscreteField;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.model.Fields;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import net.engio.mbassy.bus.MBassador;

@DirtiesContext
@ContextConfiguration(classes = {TestMarketDataServiceConfig.class})
public class ModelDataCassandraSinkITCase extends AbstractTestNGSpringContextTests {

  @Autowired
  private CassandraOperations session;

  @Qualifier("modelData")
  @Autowired
  private MBassador<Map<String, Object>> modelDataBus;

  @Autowired
  private ModelDataDao dao;

  @AfterMethod
  public void cleanup() {
    session.getSession().execute("TRUNCATE modeldata");
  }

  @Test
  public void testStoreModelData() throws Exception {

    Map<String, Object> tuple = new HashMap<String, Object>();
    Candle c = new Candle("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);
    DiscreteField df = new DiscreteField("google", Instant.now(), 100, "ave", "google");
    ContinousField cf = new ContinousField("google", Instant.now(), BigDecimal.TEN, "sma", "google");
    CategoricalField cat = new CategoricalField("goog", Instant.now(), "category", "some field name", "goog");
    
    tuple.put(Fields.MARKETDATA.toString(), c);
    tuple.put(Fields.AVE.toString(), Lists.newArrayList(df));
    tuple.put(Fields.SMA.toString(), Lists.newArrayList(cf));
    tuple.put(Fields.MODEL_NAME.toString(), "test model");
    modelDataBus.publish(tuple);

    Thread.sleep(5000);

    long count = dao.count();

    assertEquals(count, 1);

    Iterable<ModelDataDto> result = dao.findAll();

    ModelDataDto dto = result.iterator().next();

    Assert.assertNotNull(dto.primarykey);
    Assert.assertNotNull(dto.primarykey.date);
    Assert.assertNotNull(dto.primarykey.timestamp);
    Assert.assertNotNull(dto.primarykey.modelName);
    Assert.assertNotNull(dto.fields);
    Assert.assertEquals(dto.fields.size(), 10);

    Map<String, Object> map = dto.fromRow();
    MarketData data = (MarketData) map.get(Fields.MARKETDATA.toString());
    String modelName = (String) map.get(Fields.MODEL_NAME.toString());
    
    Assert.assertNotNull(data);
    Assert.assertNotNull(modelName);
    Assert.assertNotNull(map.get(Fields.AVE.toString()));
    Assert.assertNotNull(map.get(Fields.SMA.toString()));
    Assert.assertEquals(c, data);
    Assert.assertEquals(((Collection)map.get(Fields.SMA.toString())).iterator().next(), cf);
    Assert.assertEquals(((Collection)map.get(Fields.AVE.toString())).iterator().next(), df);
    
  }
}
