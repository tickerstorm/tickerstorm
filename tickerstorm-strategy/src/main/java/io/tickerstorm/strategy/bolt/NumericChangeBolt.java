package io.tickerstorm.strategy.bolt;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.util.CacheManager;
import net.sf.ehcache.Element;

@SuppressWarnings("serial")
public class NumericChangeBolt extends BaseBolt {

  private final static String CACHE = "md-cache";

  @Override
  protected void process(Tuple input) {

    Set<Field<BigDecimal>> abs = new HashSet<>();
    Set<Field<BigDecimal>> pct = new HashSet<>();

    if (input.contains(Field.Name.DISCRETE_FIELDS.field())) {

      Collection<Field<Integer>> current = (Collection<Field<Integer>>) input.getValueByField(Field.Name.DISCRETE_FIELDS.field());

      for (Field<Integer> f : current) {
        
        List<Field<Integer>> previous = (List) cache(f);
        Collections.sort(previous);
        
        int i = previous.indexOf(f);
        Field<Integer> prior = null;
        int p = 2;

        if (i >= 0 && previous.size() >= (i + (p+1))) {
          
          prior = (Field<Integer>) previous.get(i + p);
          Integer absDiff = (f.getValue() - prior.getValue());
          BigDecimal pctDiff = BigDecimal.valueOf(absDiff).divide(BigDecimal.valueOf(prior.getValue()), 4, BigDecimal.ROUND_HALF_UP);
          abs.add(new BaseField<>(f, Field.Name.ABS_CHANGE.field(), BigDecimal.valueOf(absDiff)));
          pct.add(new BaseField<>(f, Field.Name.PCT_CHANGE.field(), pctDiff));
        
        } else {
          
          abs.add(new BaseField(f, Field.Name.ABS_CHANGE.field(), BigDecimal.class));
          pct.add(new BaseField(f, Field.Name.PCT_CHANGE.field(), BigDecimal.class));
        
        }

      }
    }

    if (input.contains(Field.Name.CONTINOUS_FIELDS.field())) {

      Collection<Field<BigDecimal>> current = (Collection<Field<BigDecimal>>) input.getValueByField(Field.Name.CONTINOUS_FIELDS.field());

      for (Field<BigDecimal> f : current) {

        List<Field<BigDecimal>> previous = (List) cache(f);
        Collections.sort(previous);
        
        int i = previous.indexOf(f);
        Field<BigDecimal> prior = null;
        BigDecimal absDiff = BigDecimal.ZERO;
        BigDecimal pctDiff = BigDecimal.ZERO;
        int p = 2;

        if (i >= 0 && previous.size() >= (i + (p+1))) {

          prior = (Field<BigDecimal>) previous.get(i + p);

          absDiff = (f.getValue().subtract(prior.getValue()));
          pctDiff = absDiff.divide(prior.getValue(), 4, BigDecimal.ROUND_HALF_UP);
          abs.add(new BaseField<>(f, Field.Name.ABS_CHANGE.field(), absDiff));
          pct.add(new BaseField<>(f, Field.Name.PCT_CHANGE.field(), pctDiff));

        } else {
          abs.add(new BaseField(f, Field.Name.ABS_CHANGE.field(), BigDecimal.class));
          pct.add(new BaseField(f, Field.Name.PCT_CHANGE.field(), BigDecimal.class));
        }

      }
    }

    coll.emit(input, new Values(abs, pct));

    ack(input);
  }

  List<Field<?>> cache(Field<?> f) {

    final String key = CacheManager.buildKey(f).toString();
    CacheManager.getInstance(CACHE).putIfAbsent(new Element(key, new ArrayList<Field<?>>()));
    List<Field<?>> previous = new ArrayList<>();

    try {

      CacheManager.getInstance(CACHE).acquireWriteLockOnKey(key);
      previous = (List<Field<?>>) CacheManager.getInstance(CACHE).get(key).getObjectValue();
      previous.add(f);
      CacheManager.getInstance(CACHE).put(new Element(key, previous));

    } finally {

      CacheManager.getInstance(CACHE).releaseWriteLockOnKey(key);

    }
    
    return previous;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Field.Name.PCT_CHANGE.field(), Field.Name.ABS_CHANGE.field()));
  }

}
