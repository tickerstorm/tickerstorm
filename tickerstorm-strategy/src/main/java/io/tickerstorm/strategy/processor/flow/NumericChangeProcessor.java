package io.tickerstorm.strategy.processor.flow;


import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.collections.SynchronizedIndexedTreeMap;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;

@Component
public class NumericChangeProcessor extends BaseEventProcessor {

  public final static String METRIC_TIME_TAKEN = "metric.numericchange.time";

  public static final String PERIODS_CONFIG_KEY = "proc.numericchange.periods";

  private Predicate<Field<?>> filter() {
    return p -> (BigDecimal.class.isAssignableFrom(p.getFieldType()) || Integer.class.isAssignableFrom(p.getFieldType()))
        && !p.getName().contains(Field.Name.ABS_CHANGE.field()) && !p.getName().contains(Field.Name.PCT_CHANGE.field()) && !p.isNull();
  }

  @Subscribe
  public void handle(Collection<Field<?>> fss) {

    long start = System.currentTimeMillis();

    Set<Field<?>> fs = new HashSet<>();
    fss.stream().filter(filter()).forEach(f -> {

      int p = Integer.valueOf((String) getConfig(f.getStream()).getOrDefault(PERIODS_CONFIG_KEY, "2"));

      BigDecimal absDiff = BigDecimal.ZERO;
      BigDecimal pctDiff = BigDecimal.ZERO;

      long start2 = System.currentTimeMillis();
      CacheManager.put(f, p);
      SynchronizedIndexedTreeMap<Field<?>> cache = CacheManager.getFieldCache(f);
      Field<Number> prior = (Field) cache.get(f.getTimestamp(), p);
      logger.debug("Caching took :" + (System.currentTimeMillis() - start2) + "ms");

      if (prior != null && !prior.equals(f)) {

        long start3 = System.currentTimeMillis();
        BigDecimal priorVal = new BigDecimal(prior.getValue() + "").setScale(4, BigDecimal.ROUND_HALF_UP);
        BigDecimal fVal = new BigDecimal(f.getValue() + "").setScale(4, BigDecimal.ROUND_HALF_UP);

        absDiff = fVal.subtract(priorVal).setScale(4, BigDecimal.ROUND_HALF_UP);

        if (absDiff.compareTo(BigDecimal.ZERO) != 0 && priorVal.compareTo(BigDecimal.ZERO) != 0) {
          pctDiff = absDiff.divide(priorVal, 4, BigDecimal.ROUND_HALF_UP);
          fs.add(new BaseField<>(f, Field.Name.PCT_CHANGE.field() + "-p" + p, pctDiff.setScale(4, BigDecimal.ROUND_HALF_UP)));
        } else {
          fs.add(new BaseField<>(f, Field.Name.PCT_CHANGE.field() + "-p" + p, BigDecimal.ZERO.setScale(4, BigDecimal.ROUND_HALF_UP)));
        }

        fs.add(new BaseField<>(f, Field.Name.ABS_CHANGE.field() + "-p" + p, absDiff.setScale(4, BigDecimal.ROUND_HALF_UP)));
        logger.debug("Computation took :" + (System.currentTimeMillis() - start3) + "ms");
      }
    });

    logger.debug("Numberic Change Processor took :" + (System.currentTimeMillis() - start) + "ms");

    if (!fs.isEmpty()) {
      publish(fs);
      logger.debug("Computed " + fs.size() + " values");
    }

    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));

  }

}
