package io.tickerstorm.strategy.bolt;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.util.CacheManager;
import net.sf.ehcache.Element;

@Component
@SuppressWarnings("serial")
public class BasicStatsBolt extends BaseBolt {

  private final static Logger logger = LoggerFactory.getLogger(BasicStatsBolt.class);
  private List<Integer> periods = null;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer dec) {
    dec.declare(new Fields("simple-statistics"));
  }

  @Override
  protected void process(Tuple tuple) {

    HashSet<Field<?>> fields = new HashSet<>();
    Set<Field<BigDecimal>> bg = new HashSet<>();
    Set<Field<Integer>> is = new HashSet<>();

    if (tuple.contains(Field.Name.CONTINOUS_FIELDS.field()))
      bg = (Set<Field<BigDecimal>>) tuple.getValueByField(Field.Name.CONTINOUS_FIELDS.field());

    if (tuple.contains(Field.Name.DISCRETE_FIELDS.field()))
      is = (Set<Field<Integer>>) tuple.getValueByField(Field.Name.DISCRETE_FIELDS.field());

    for (Integer p : periods) {

      for (Field<BigDecimal> f : bg) {

        DescriptiveStatistics ds = cache(CacheManager.buildKey(f).toString(), f, p);

        if (ds.getValues().length == p) {
          fields.add(new BaseField<BigDecimal>(f, Field.Name.MAX.field() + "-p" + p, new BigDecimal(ds.getMax())));
          fields.add(new BaseField<BigDecimal>(f, Field.Name.MIN.field() + "-p" + p, new BigDecimal(ds.getMin())));
          fields.add(new BaseField<BigDecimal>(f, Field.Name.SMA.field() + "-p" + p, new BigDecimal(ds.getMean())));
          fields.add(new BaseField<BigDecimal>(f, Field.Name.STD.field() + "-p" + p, new BigDecimal(ds.getStandardDeviation())));
        } else {
          fields.add(new BaseField<BigDecimal>(f, Field.Name.MAX.field() + "-p" + p, BigDecimal.class));
          fields.add(new BaseField<BigDecimal>(f, Field.Name.MIN.field() + "-p" + p, BigDecimal.class));
          fields.add(new BaseField<BigDecimal>(f, Field.Name.SMA.field() + "-p" + p, BigDecimal.class));
          fields.add(new BaseField<BigDecimal>(f, Field.Name.STD.field() + "-p" + p, BigDecimal.class));
        }
      }

      for (Field<Integer> f : is) {

        DescriptiveStatistics ds = cache(CacheManager.buildKey(f).toString(), f, p);

        // Only compute statistics if we have enough periods to compute on in the array
        if (ds.getValues().length == p) {
          fields.add(new BaseField<BigDecimal>(f, Field.Name.MAX.field() + "-p" + p, new BigDecimal(ds.getMax())));
        } else {
          fields.add(new BaseField<BigDecimal>(f, Field.Name.MAX.field() + "-p" + p, BigDecimal.class));
        }
      }
    }
    coll.emit(tuple, new Values(fields));
    ack(tuple);
  }

  private SynchronizedDescriptiveStatistics cache(String key, Field<?> f, int period) {
    SynchronizedDescriptiveStatistics q = null;
    try {

      CacheManager.getInstance("md-cache").acquireWriteLockOnKey(key);
      CacheManager.getInstance("md-cache").putIfAbsent(new Element(key, new SynchronizedDescriptiveStatistics(period)));

      q = (SynchronizedDescriptiveStatistics) CacheManager.getInstance("md-cache").get(key).getObjectValue();

      if (f.getFieldType().isAssignableFrom(BigDecimal.class))
        q.addValue(((BigDecimal) f.getValue()).doubleValue());

      if (f.getFieldType().isAssignableFrom(Integer.class))
        q.addValue(((Integer) f.getValue()).doubleValue());

    } finally {
      CacheManager.getInstance("md-cache").releaseWriteLockOnKey(key);
    }

    return q;
  }

  @Override
  protected void init() {

    periods = Lists.newArrayList(1, 10, 15, 30, 60, 90);

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
}
