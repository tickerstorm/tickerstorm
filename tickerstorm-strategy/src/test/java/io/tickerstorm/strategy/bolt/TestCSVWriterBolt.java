package io.tickerstorm.strategy.bolt;

import static io.tickerstorm.common.entity.Field.Name.MARKER;
import static io.tickerstorm.common.entity.Field.Name.MARKETDATA;
import static io.tickerstorm.common.entity.Field.Name.SMA;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Markers;

public class TestCSVWriterBolt {

  private CSVWriterBolt bolt;

  @Mock
  private OutputCollector collector;

  @Mock
  private TopologyContext context;

  private static final String fileName = "/tmp/testfile.csv";

  @BeforeMethod
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    bolt = new CSVWriterBolt();
    java.util.Map<String, String> config = new java.util.HashMap<>();
    config.put("output.file.csv.name", fileName);
    Mockito.doNothing().when(collector).ack(Mockito.any(Tuple.class));
    bolt.prepare(config, context, collector);

    backtype.storm.tuple.Fields f = new backtype.storm.tuple.Fields(MARKER.field(), MARKETDATA.field(), SMA.field(), "category");

    Mockito.doReturn("1").when(context).getComponentId(1);
    Mockito.doReturn(f).when(context).getComponentOutputFields("1", "1");
  }

  @Test
  public void testWriteCSVFile() throws Exception {

    Instant now = Instant.now();

    List<Object> values = new ArrayList<>();
    Command m = new Command("TestCase", Markers.SESSION_START.toString());
    m.config.put("output.file.csv.path", fileName);
    values.add(m);
    Candle md = new Candle("TOL", "Google", now, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, Candle.MIN_1_INTERVAL,
        Integer.MAX_VALUE);
    values.add(md);

    values.add(Sets.newHashSet(new BaseField<BigDecimal>(md.getEventId(), "sma", BigDecimal.ONE)));

    values.add(Sets.newHashSet(new BaseField<String>(md.getEventId(), "category", "Some String")));

    Tuple t = new TupleImpl(context, values, 1, "1");
    bolt.execute(t);

    Assert.assertTrue(new File(fileName).exists());

    int i = 0;
    for (String l : IOUtils.readLines(new FileInputStream(new File(fileName)))) {
      if (i == 0) {
        Assert.assertTrue(l.contains("symbol"));
        Assert.assertTrue(l.contains("timestamp"));
        Assert.assertTrue(l.contains("source"));
        Assert.assertTrue(l.contains("open"));
        Assert.assertTrue(l.contains("close"));
        Assert.assertTrue(l.contains("category"));
        Assert.assertTrue(l.contains("high"));
        Assert.assertTrue(l.contains("low"));
        Assert.assertTrue(l.contains("volume"));
        Assert.assertTrue(l.contains("sma"));
      }

      if (i > 0) {
        Assert.assertFalse(l.contains("open"));
        Assert.assertFalse(l.contains("close"));
        Assert.assertFalse(l.contains("category"));
        Assert.assertFalse(l.contains("high"));
        Assert.assertFalse(l.contains("low"));
        Assert.assertFalse(l.contains("volume"));
        Assert.assertFalse(l.contains("sma"));

        Assert.assertTrue(l.contains("0"));
        Assert.assertTrue(l.contains("10"));
        Assert.assertTrue(l.contains("Some String"));
        Assert.assertTrue(l.contains(Integer.MAX_VALUE + ""));
        Assert.assertTrue(l.contains(now + ""));
        Assert.assertTrue(l.contains("Google"));
        Assert.assertTrue(l.contains("TOL"));
      }

      i++;
    }

  }

  @AfterMethod
  public void cleanup() {
    if (new File(fileName).exists())
      new File(fileName).delete();
  }

}
