/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.strategy.processor.flow;


import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.collections.SynchronizedIndexedTreeMap;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.processor.BaseEventProcessor;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import org.springframework.stereotype.Component;

@Component
public class NumericChangeProcessor extends BaseEventProcessor {

  public final static String METRIC_TIME_TAKEN = "metric.numericchange.time";
  public static final String PERIODS_CONFIG_KEY = "periods";

  private Predicate<Field<?>> filter() {
    return p -> (BigDecimal.class.isAssignableFrom(p.getFieldType()) || Integer.class.isAssignableFrom(p.getFieldType()))
        && !p.getName().contains(Field.Name.ABS_CHANGE.field()) && !p.getName().contains(Field.Name.PCT_CHANGE.field()) && !p.isNull();
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

    Set<Field<?>> fs = new HashSet<>();

    TransformerConfig config = (TransformerConfig) getConfig(f.getStream()).get(TRANSFORMER_CONFIG_KEY);
    Set<Integer> ps = config.findPeriod(f.getSymbol(), f.getInterval());

    BigDecimal absDiff = BigDecimal.ZERO;
    BigDecimal pctDiff = BigDecimal.ZERO;

    for (Integer p : ps) {

      long start2 = System.currentTimeMillis();
      SynchronizedIndexedTreeMap<Field<?>> cache = CacheManager.cache(f);
      Field<Number> prior = (Field) cache.get(f.getTimestamp(), p);
      logger.trace("Caching took :" + (System.currentTimeMillis() - start2) + "ms");

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
        logger.trace("Computation took :" + (System.currentTimeMillis() - start3) + "ms");

        if (!fs.isEmpty()) {
          publish(fs);
          logger.trace("Numberic Change Processor took :" + (System.currentTimeMillis() - start) + "ms to compute " + fs.size());
        }
      }
    }

    gauge.submit(METRIC_TIME_TAKEN, (System.currentTimeMillis() - start));

  }

  @Override
  public String name() {
    return "numeric-change";
  }

}
