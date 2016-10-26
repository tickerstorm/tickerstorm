package io.tickerstorm.strategy.processor.flow;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;
import net.sf.ehcache.Element;

@Component
public class BasicStatsProcessor extends BaseEventProcessor {

  public final static String METRIC_TIME_TAKEN = "metric.basicstats.time";

  private Predicate<Field<?>> filter() {
    return p -> (BigDecimal.class.isAssignableFrom(p.getFieldType()) || Integer.class.isAssignableFrom(p.getFieldType()))
        && !p.getName().contains(Field.Name.MAX.field()) && !p.getName().contains(Field.Name.MIN.field())
        && !p.getName().contains(Field.Name.SMA.field()) && !p.getName().contains(Field.Name.STD.field()) && !p.isNull();
  }

  @Subscribe
  public void handle(Collection<Field<?>> fss) {

    long start = System.currentTimeMillis();

    final Set<Field<?>> fs = new HashSet<>();
    fss.stream().filter(filter()).forEach(f -> {

      List<Integer> periods = (List<Integer>) getConfig(f.getStream()).getOrDefault(METRIC_TIME_TAKEN, Lists.newArrayList(30));

      try {
        for (Integer p : periods) {

          DescriptiveStatistics ds = cacheField(f, p);

          if (ds.getValues().length == p) {
            fs.add(new BaseField<BigDecimal>(f, Field.Name.MAX.field() + "-p" + p,
                new BigDecimal(ds.getMax()).setScale(4, BigDecimal.ROUND_HALF_UP)));
            fs.add(new BaseField<BigDecimal>(f, Field.Name.MIN.field() + "-p" + p,
                new BigDecimal(ds.getMin()).setScale(4, BigDecimal.ROUND_HALF_UP)));
            fs.add(new BaseField<BigDecimal>(f, Field.Name.SMA.field() + "-p" + p,
                new BigDecimal(ds.getMean()).setScale(4, BigDecimal.ROUND_HALF_UP)));
            fs.add(new BaseField<BigDecimal>(f, Field.Name.STD.field() + "-p" + p,
                new BigDecimal(ds.getStandardDeviation()).setScale(4, BigDecimal.ROUND_HALF_UP)));
          }
        }
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    });

    if (!fs.isEmpty())
      publish(fs);

    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));
    logger.trace("Basic Stats Processor took :" + (System.currentTimeMillis() - start) + "ms");
  }

  private SynchronizedDescriptiveStatistics cacheField(Field<?> f, int period) {

    String key = CacheManager.buildKey(f).toString() + "-p" + period;
    SynchronizedDescriptiveStatistics q = null;
    try {

      CacheManager.getInstance(f.getStream()).putIfAbsent(new Element(key, new SynchronizedDescriptiveStatistics(period)));

      CacheManager.getInstance(f.getStream()).acquireWriteLockOnKey(key);

      q = (SynchronizedDescriptiveStatistics) CacheManager.getInstance(f.getStream()).get(key).getObjectValue();

      if (f.getFieldType().isAssignableFrom(BigDecimal.class))
        q.addValue(((BigDecimal) f.getValue()).doubleValue());

      if (f.getFieldType().isAssignableFrom(Integer.class))
        q.addValue(((Integer) f.getValue()).doubleValue());

    } finally {
      CacheManager.getInstance(f.getStream()).releaseWriteLockOnKey(key);
    }

    return q;

  }

  @Override
  public String name() {
    return "basic-stats";
  }


}
