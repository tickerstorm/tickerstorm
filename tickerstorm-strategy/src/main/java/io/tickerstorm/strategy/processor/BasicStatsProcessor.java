package io.tickerstorm.strategy.processor;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;

import javax.annotation.PostConstruct;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.util.CacheManager;

@Component
public class BasicStatsProcessor extends BaseEventProcessor {

  private List<Integer> periods = null;

  @PostConstruct
  protected void init() {
    super.init();

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

  private Predicate<Field<?>> filter() {
    return p -> BigDecimal.class.isAssignableFrom(p.getFieldType()) && Integer.class.isAssignableFrom(p.getFieldType())
        && !p.getName().contains(Field.Name.MAX.field()) && !p.getName().contains(Field.Name.MIN.field())
        && !p.getName().contains(Field.Name.SMA.field()) && !p.getName().contains(Field.Name.STD.field());
  }

  @Subscribe
  public void handle(Field<?> f) throws Exception {

    if (!filter().test(f))
      return;

    HashSet<Field<?>> fields = new HashSet<>();

    for (Integer p : periods) {

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
  }


}
