package io.tickerstorm.strategy.processor.flow;

import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;
import java.math.BigDecimal;
import java.nio.file.AtomicMoveNotSupportedException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import net.sf.ehcache.Element;
import org.apache.commons.math3.stat.descriptive.ComparableDescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.springframework.stereotype.Component;

@Component
public class BasicStatsProcessor extends BaseEventProcessor {

  public final static String METRIC_TIME_TAKEN = "metric.basicstats.time";

  private Predicate<Field<?>> filter() {
    return p -> (BigDecimal.class.isAssignableFrom(p.getFieldType()) || Integer.class.isAssignableFrom(p.getFieldType()))
        && !p.getName().contains(Field.Name.MAX.field()) && !p.getName().contains(Field.Name.MIN.field())
        && !p.getName().contains(Field.Name.SMA.field()) && !p.getName().contains(Field.Name.STD.field()) && !p.isNull() && isActive(p.getStream());
  }

  @Subscribe
  public void handle(Collection<Field<?>> fss) {
    fss.stream().filter(filter()).forEach(f -> {
      handle(f);
    });
  }

  @Subscribe
  public void handle(Field<?> f) {

    if (!filter().test(f)) {
      return;
    }

    long start = System.currentTimeMillis();
    final Set<Field<?>> fs = new HashSet<>();

    TransformerConfig config = (TransformerConfig) getConfig(f.getStream()).get(TRANSFORMER_CONFIG_KEY);
    Set<Integer> periods = config.findPeriod(f.getSymbol(), f.getInterval());

    for (Integer p : periods) {

      ComparableDescriptiveStatistics ds = cacheField(f, p);

      if (ds.isFull()) {
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

    if (!fs.isEmpty()) {
      publish(fs);
    }

    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));
    logger.trace("Basic Stats Processor took :" + (System.currentTimeMillis() - start) + "ms");
  }

  private ComparableDescriptiveStatistics cacheField(Field<?> f, int period) {

    String key = CacheManager.buildKey(f).toString() + "-p" + period;
    ComparableDescriptiveStatistics q = null;

    final AtomicBoolean replaced = new AtomicBoolean(false);
    CacheManager.getInstance(f.getStream().toLowerCase()).putIfAbsent(new Element(key, new ComparableDescriptiveStatistics(period)));

    while (!replaced.get()) {

      Element e = CacheManager.getInstance(f.getStream().toLowerCase()).get(key);

      q = ((ComparableDescriptiveStatistics) e.getObjectValue()).copy();

      if (f.getFieldType().isAssignableFrom(BigDecimal.class)) {
        q.addValue(((BigDecimal) f.getValue()).doubleValue());
      } else if (f.getFieldType().isAssignableFrom(Integer.class)) {
        q.addValue(((Integer) f.getValue()).doubleValue());
      }

      replaced.set(CacheManager.getInstance(f.getStream().toLowerCase()).replace(e, new Element(key, q)));

      if (!replaced.get()) {
        logger.warn("Retempting cache update for " + key);
      }
    }

    return q;

  }

  @Override
  public String name() {
    return "basic-stats";
  }


}
