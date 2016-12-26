package io.tickerstorm.strategy.processor.flow;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.config.TransformerConfig;
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
    fss.stream().filter(filter()).forEach(f -> {
      handle(f);
    });
  }

  @Subscribe
  public void handle(Field<?> f) {

    if (!filter().test(f))
      return;

    long start = System.currentTimeMillis();
    final Set<Field<?>> fs = new HashSet<>();

    TransformerConfig config = (TransformerConfig) getConfig(f.getStream()).get(TRANSFORMER_CONFIG_KEY);
    Set<Integer> periods = config.findPeriod(f.getSymbol(), f.getInterval());

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

    if (!fs.isEmpty())
      publish(fs);

    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));
    logger.trace("Basic Stats Processor took :" + (System.currentTimeMillis() - start) + "ms");
  }

  private SynchronizedDescriptiveStatistics cacheField(Field<?> f, int period) {

    String key = CacheManager.buildKey(f).toString() + "-p" + period;
    SynchronizedDescriptiveStatistics q = null;

    Element e =
        CacheManager.getInstance(f.getStream().toLowerCase()).putIfAbsent(new Element(key, new SynchronizedDescriptiveStatistics(period)));

    if (e == null)
      e = CacheManager.getInstance(f.getStream().toLowerCase()).get(key);

    q = (SynchronizedDescriptiveStatistics) e.getObjectValue();

    if (f.getFieldType().isAssignableFrom(BigDecimal.class))
      q.addValue(((BigDecimal) f.getValue()).doubleValue());

    if (f.getFieldType().isAssignableFrom(Integer.class))
      q.addValue(((Integer) f.getValue()).doubleValue());

    CacheManager.getInstance(f.getStream().toLowerCase()).put(new Element(key, q));

    return q;

  }

  @Override
  public String name() {
    return "basic-stats";
  }


}
