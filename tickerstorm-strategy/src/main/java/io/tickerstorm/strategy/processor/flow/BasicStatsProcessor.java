package io.tickerstorm.strategy.processor.flow;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;
import io.tickerstorm.strategy.util.CacheManager;
import net.sf.ehcache.Element;

@Component
public class BasicStatsProcessor extends BaseEventProcessor {

  private List<Integer> periods = Lists.newArrayList(1, 10, 15, 30, 60, 90);

  private Predicate<Field<?>> filter() {
    return p -> (BigDecimal.class.isAssignableFrom(p.getFieldType()) || Integer.class.isAssignableFrom(p.getFieldType()))
        && !p.getName().contains(Field.Name.MAX.field()) && !p.getName().contains(Field.Name.MIN.field())
        && !p.getName().contains(Field.Name.SMA.field()) && !p.getName().contains(Field.Name.STD.field()) && !p.isNull();
  }

  @Subscribe
  public void handle(Field<?> f) throws Exception {

    if (!filter().test(f))
      return;

    try {
      for (Integer p : periods) {

        DescriptiveStatistics ds = cacheField(f, p);

        if (ds.getValues().length == p) {
          publish(new BaseField<BigDecimal>(f, Field.Name.MAX.field() + "-p" + p, new BigDecimal(ds.getMax())));
          publish(new BaseField<BigDecimal>(f, Field.Name.MIN.field() + "-p" + p, new BigDecimal(ds.getMin())));
          publish(new BaseField<BigDecimal>(f, Field.Name.SMA.field() + "-p" + p, new BigDecimal(ds.getMean())));
          publish(new BaseField<BigDecimal>(f, Field.Name.STD.field() + "-p" + p, new BigDecimal(ds.getStandardDeviation())));
        } 
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  private SynchronizedDescriptiveStatistics cacheField(Field<?> f, int period) {

    String key = CacheManager.buildKey(f).toString() + "-p" + period;
    SynchronizedDescriptiveStatistics q = null;
    try {

      CacheManager.getInstance(CACHE).putIfAbsent(new Element(key, new SynchronizedDescriptiveStatistics(period)));

      CacheManager.getInstance(CACHE).acquireWriteLockOnKey(key);

      q = (SynchronizedDescriptiveStatistics) CacheManager.getInstance(CACHE).get(key).getObjectValue();

      if (f.getFieldType().isAssignableFrom(BigDecimal.class))
        q.addValue(((BigDecimal) f.getValue()).doubleValue());

      if (f.getFieldType().isAssignableFrom(Integer.class))
        q.addValue(((Integer) f.getValue()).doubleValue());

    } finally {
      CacheManager.getInstance(CACHE).releaseWriteLockOnKey(key);
    }

    return q;

  }


}
