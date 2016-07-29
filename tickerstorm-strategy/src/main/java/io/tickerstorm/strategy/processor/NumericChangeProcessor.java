package io.tickerstorm.strategy.processor;


import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.google.common.eventbus.Subscribe;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;

@Component
public class NumericChangeProcessor extends BaseEventProcessor {

  private Predicate<Field<?>> filter() {
    return p -> BigDecimal.class.isAssignableFrom(p.getFieldType()) && Integer.class.isAssignableFrom(p.getFieldType())
        && !p.getName().contains(Field.Name.ABS_CHANGE.field()) && !p.getName().contains(Field.Name.PCT_CHANGE.field());
  }

  @Subscribe
  public void handle(Field<?> f) throws Exception {

    if (!filter().test(f))
      return;

    if (BigDecimal.class.isAssignableFrom(f.getFieldType()) || Integer.class.isAssignableFrom(f.getFieldType())) {

      List<Field<?>> previous = (List) cache(f);
      Collections.sort(previous);

      int i = previous.indexOf(f);
      Field<?> prior = null;
      BigDecimal absDiff = BigDecimal.ZERO;
      BigDecimal pctDiff = BigDecimal.ZERO;
      int p = 2;

      if (i >= 0 && previous.size() >= (i + (p + 1))) {

        prior = (Field<?>) previous.get(i + p);

        absDiff = (new BigDecimal(f.getValue() + "").subtract(new BigDecimal(f.getValue() + "")));
        pctDiff = absDiff.divide(new BigDecimal(prior.getValue() + ""), 4, BigDecimal.ROUND_HALF_UP);
        publish(new BaseField<>(f, Field.Name.ABS_CHANGE.field(), absDiff));
        publish(new BaseField<>(f, Field.Name.PCT_CHANGE.field(), pctDiff));

      } else {
        publish(new BaseField(f, Field.Name.ABS_CHANGE.field(), BigDecimal.class));
        publish(new BaseField(f, Field.Name.PCT_CHANGE.field(), BigDecimal.class));
      }

    }
  }


}
