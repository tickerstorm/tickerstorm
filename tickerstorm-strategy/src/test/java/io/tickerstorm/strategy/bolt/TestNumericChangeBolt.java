package io.tickerstorm.strategy.bolt;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;

public class TestNumericChangeBolt {

  private final String STREAM_ID = "1";
  private final Integer TASK_ID = 1;

  @Mock
  private OutputCollector collector;

  @Mock
  private TopologyContext context;

  private NumericChangeBolt bolt;

  @BeforeMethod
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    bolt = new NumericChangeBolt();
    java.util.Map<String, String> config = new java.util.HashMap<>();
    Mockito.doNothing().when(collector).ack(Mockito.any(Tuple.class));
    bolt.prepare(config, context, collector);
  }

  @Test
  public void testComputeFirstDiscreteChange() {

    Fields def = new Fields(Field.Name.DISCRETE_FIELDS.field());
    Mockito.doReturn("1").when(context).getComponentId(1);
    Mockito.doReturn(def).when(context).getComponentOutputFields("1", "1");

    ArrayList<Field<Integer>> fs = new ArrayList<>();
    Candle md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Candle.MIN_1_INTERVAL, Integer.MAX_VALUE);
    md.stream = "TestNumericChangeBolt";

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().equals(Integer.class))
        fs.add((Field<Integer>) f);
    }

    Assert.assertFalse(fs.isEmpty());

    List<Object> values = new ArrayList<>();
    values.add(fs);
    TupleImpl t = new TupleImpl(context, values, TASK_ID, STREAM_ID);

    bolt.execute(t);

    ArgumentCaptor<Values> emit = ArgumentCaptor.forClass(Values.class);
    Mockito.verify(collector, Mockito.times(1)).emit(Mockito.any(Tuple.class), emit.capture());

    Values args = emit.getValue();

    Assert.assertNotNull(args);
    Assert.assertEquals(2, args.size());

    Field<BigDecimal> abs = ((Set<Field<BigDecimal>>) args.get(0)).iterator().next();
    Field<BigDecimal> pct = ((Set<Field<BigDecimal>>) args.get(1)).iterator().next();

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.ABS_CHANGE.field(), BigDecimal.class);
    BaseField<BigDecimal> f2 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.PCT_CHANGE.field(), BigDecimal.class);

    Assert.assertEquals(abs, f1);
    Assert.assertEquals(pct, f2);

  }

  @Test
  public void testComputeSecondDiscreteChange() {

    Fields def = new Fields(Field.Name.DISCRETE_FIELDS.field());
    Mockito.doReturn("1").when(context).getComponentId(1);
    Mockito.doReturn(def).when(context).getComponentOutputFields("1", "1");

    ArrayList<Field<Integer>> fs = new ArrayList<>();
    Candle md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN,
        Candle.MIN_1_INTERVAL, Integer.MAX_VALUE);
    md.stream = "TestNumericChangeBolt";

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().equals(Integer.class))
        fs.add((Field<Integer>) f);
    }

    Assert.assertFalse(fs.isEmpty());

    List<Object> values = new ArrayList<>();
    values.add(fs);
    TupleImpl t = new TupleImpl(context, values, TASK_ID, STREAM_ID);

    bolt.execute(t);

    fs = new ArrayList<>();
    md = new Candle("TOL", "Google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Candle.MIN_1_INTERVAL,
        12335253);
    md.stream = "TestNumericChangeBolt";

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().equals(Integer.class))
        fs.add((Field<Integer>) f);
    }

    Assert.assertFalse(fs.isEmpty());

    values = new ArrayList<>();
    values.add(fs);
    t = new TupleImpl(context, values, TASK_ID, STREAM_ID);

    bolt.execute(t);

    ArgumentCaptor<Values> emit = ArgumentCaptor.forClass(Values.class);
    Mockito.verify(collector, Mockito.times(2)).emit(Mockito.any(Tuple.class), emit.capture());

    List<Values> args = emit.getAllValues();

    Assert.assertNotNull(args.get(1));
    Assert.assertEquals(2, args.get(1).size());

    Field<BigDecimal> abs = ((Set<Field<BigDecimal>>) args.get(1).get(0)).iterator().next();
    Field<BigDecimal> pct = ((Set<Field<BigDecimal>>) args.get(1).get(1)).iterator().next();

    BaseField<BigDecimal> f1 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.ABS_CHANGE.field(),
        BigDecimal.valueOf(12335253 - Integer.MAX_VALUE));
    
    BaseField<BigDecimal> f2 = new BaseField<BigDecimal>(md.getEventId(), "volume|" + Field.Name.PCT_CHANGE.field(),
        BigDecimal.valueOf(12335253 - Integer.MAX_VALUE).divide(BigDecimal.valueOf(Integer.MAX_VALUE), 4, BigDecimal.ROUND_HALF_UP));

    Assert.assertEquals(abs, f1);
    Assert.assertEquals(pct, f2);

  }

}
