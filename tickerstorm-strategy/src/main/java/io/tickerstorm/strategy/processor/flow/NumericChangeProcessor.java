package io.tickerstorm.strategy.processor.flow;


import java.math.BigDecimal;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;
import io.tickerstorm.strategy.util.FieldUtil;

@Component
public class NumericChangeProcessor extends BaseEventProcessor {

  public static final String PERIODS_CONFIG_KEY = "proc.numericchange.periods";

  private Predicate<Field<?>> filter() {
    return p -> (BigDecimal.class.isAssignableFrom(p.getFieldType()) || Integer.class.isAssignableFrom(p.getFieldType()))
        && !p.getName().contains(Field.Name.ABS_CHANGE.field()) && !p.getName().contains(Field.Name.PCT_CHANGE.field()) && !p.isNull();
  }

  @Subscribe
  public void handle(Field<?> f) throws Exception {

    if (!filter().test(f))
      return;

    BigDecimal absDiff = BigDecimal.ZERO;
    BigDecimal pctDiff = BigDecimal.ZERO;

    int p = Integer.valueOf(configuration(f.getStream()).getOrDefault(PERIODS_CONFIG_KEY, "2"));

    List<Field<?>> previous = cache(f, p);
    Field<Number> prior = (Field<Number>) FieldUtil.fetch(previous, f, p);

    if (!prior.equals(f)) {

      BigDecimal priorVal = new BigDecimal(prior.getValue() + "");
      BigDecimal fVal = new BigDecimal(f.getValue() + "");

      absDiff = fVal.subtract(priorVal);

      if (!absDiff.equals(BigDecimal.ZERO) && !priorVal.equals(BigDecimal.ZERO))
        pctDiff = absDiff.divide(priorVal, 4, BigDecimal.ROUND_HALF_UP);

      publish(new BaseField<>(f, Field.Name.ABS_CHANGE.field() + "-p" + p, absDiff));
      publish(new BaseField<>(f, Field.Name.PCT_CHANGE.field() + "-p" + p, pctDiff));
    }
  }

}
