package io.tickerstorm.strategy.bolt;

import io.tickerstorm.entity.CategoricalField;
import io.tickerstorm.entity.ContinousField;
import io.tickerstorm.entity.DiscreteField;
import io.tickerstorm.entity.Field;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.EmptyField;
import io.tickerstorm.strategy.util.TupleUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

@Component
@SuppressWarnings("serial")
public class ComputeAverageBolt extends BaseRichBolt {

  private final static String MARKETDATA_CACHE = "md-cache";
  private Logger logger = null;
  private OutputCollector coll = null;
  private List<Integer> periods = null;
  private CacheManager cacheManager = null;

  private String computeFieldName(Field<?> f, Integer p, String func) {
    StringBuffer b =
        new StringBuffer(func).append("|").append(p).append("|").append(f.getSymbol()).append("|")
            .append(f.getInterval()).append("|").append(f.getName());

    return b.toString();
  }

  private String computeKey(Field<?> f, Integer p) {
    StringBuffer b =
        new StringBuffer(f.getSymbol()).append("|").append(f.getInterval()).append("|")
            .append(f.getName()).append("|").append(f.getSource());

    if (p != null) {
      b.append("|").append(p);
    }

    return b.toString();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer dec) {

    List<String> fields = new ArrayList<String>(Fields.marketdataFields());
    fields.add(Fields.AVE.fieldName());
    dec.declare(new backtype.storm.tuple.Fields(fields));

  }

  @Override
  public void execute(Tuple tuple) {

    MarketData md = (MarketData) tuple.getValueByField(Fields.MARKETDATA.fieldName());

    Set<Field<?>> fields = new java.util.HashSet<>();

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().equals(CategoricalField.TYPE))
        continue;

      for (Integer p : periods) {

        String key = computeKey(f, p);

        cacheManager.getCache(MARKETDATA_CACHE).putIfAbsent(
            new Element(key, new DescriptiveStatistics(p)));

        DescriptiveStatistics ds =
            (DescriptiveStatistics) cacheManager.getCache(MARKETDATA_CACHE).get(key)
                .getObjectValue();

        if (f.getFieldType().equals(ContinousField.TYPE)) {
          ds.addValue(((ContinousField) f).getValue().doubleValue());
        } else {
          ds.addValue(((DiscreteField) f).getValue());
        }

        if (ds.getValues().length == p) {

          ContinousField aveField =
              new ContinousField(f.getSymbol(), f.getTimestamp(), new BigDecimal(ds.getMean()),
                  computeFieldName(f, p, "ma"), f.getSource(), f.getInterval());

          ContinousField gmeanField =
              new ContinousField(f.getSymbol(), f.getTimestamp(), new BigDecimal(
                  ds.getGeometricMean()), computeFieldName(f, p, "geo-mean"), f.getSource(),
                  f.getInterval());

          ContinousField maxField =
              new ContinousField(f.getSymbol(), f.getTimestamp(), new BigDecimal(ds.getMax()),
                  computeFieldName(f, p, "max"), f.getSource(), f.getInterval());

          ContinousField minField =
              new ContinousField(f.getSymbol(), f.getTimestamp(), new BigDecimal(ds.getMin()),
                  computeFieldName(f, p, "min"), f.getSource(), f.getInterval());

          fields.add(aveField);
          fields.add(gmeanField);
          fields.add(maxField);
          fields.add(minField);

        } else {

          EmptyField field =
              new EmptyField(f.getSymbol(), f.getTimestamp(), computeFieldName(f, p, "ma"),
                  f.getSource(), f.getInterval());
          fields.add(field);

          field =
              new EmptyField(f.getSymbol(), f.getTimestamp(), computeFieldName(f, p,
                  "geo-mean"), f.getSource(), f.getInterval());
          fields.add(field);

          field =
              new EmptyField(f.getSymbol(), f.getTimestamp(), computeFieldName(f, p, "max"),
                  f.getSource(), f.getInterval());
          fields.add(field);

          field =
              new EmptyField(f.getSymbol(), f.getTimestamp(), computeFieldName(f, p, "min"),
                  f.getSource(), f.getInterval());
          fields.add(field);

        }
      }
    }

    List<Object> values = TupleUtil.propagateTuple(tuple, Lists.newArrayList());
    values.add(fields);

    coll.emit(tuple, new Values(values.toArray()));
    coll.ack(tuple);

  }

  @Override
  public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
    this.coll = arg2;
    init();

    logger = LoggerFactory.getLogger(ComputeAverageBolt.class);
    periods = Lists.newArrayList(10, 15, 30, 60, 90);

    periods.sort(new Comparator<Integer>() {

      @Override
      public int compare(Integer o1, Integer o2) {

        if (o1 > o2)
          return -1;

        if (o2 > o1)
          return 1;

        return 0;
      }

    });

  }

  private void init() {

    CacheConfiguration config =
        new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
            .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO)
            .persistence(new PersistenceConfiguration().strategy(Strategy.NONE));
    config.setName("md-cache");
    cacheManager = CacheManager.create();
    cacheManager.addCache(new Cache(config));
  }

}
