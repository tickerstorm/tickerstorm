package io.tickerstorm.strategy.bolt;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.BaseField;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.strategy.util.CacheManager;
import io.tickerstorm.strategy.util.TupleUtil;
import net.sf.ehcache.Element;

@Component
@SuppressWarnings("serial")
public class ComputeAverageBolt extends BaseBolt {

  private final static Logger logger = LoggerFactory.getLogger(ComputeAverageBolt.class);
  private List<Integer> periods = null;

  private String computeFieldName(Candle md, Field<?> f, Integer p, String func) {
    StringBuffer b = new StringBuffer(func).append("|").append(p).append("|").append(md.getSymbol()).append("|").append(md.getInterval())
        .append("|").append(f.getName());

    return b.toString();
  }

  private String computeKey(Candle md, Field<?> f, Integer p) {
    StringBuffer b = new StringBuffer(md.getSymbol()).append("|").append(md.getInterval()).append("|").append(f.getName()).append("|")
        .append(md.getSource());

    if (p != null) {
      b.append("|").append(p);
    }

    return b.toString();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer dec) {

    List<String> fields = new ArrayList<String>(TupleUtil.marketdataFields());
    fields.add(Field.Name.AVE.field());
    dec.declare(new backtype.storm.tuple.Fields(fields));

  }

  @Override
  protected void process(Tuple input) {
    ack();
  }

  @Override
  protected void executeMarketData(Tuple tuple) {

    Candle md = (Candle) tuple.getValueByField(Field.Name.MARKETDATA.field());

    Set<Field<?>> fields = new java.util.HashSet<>();

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().equals(String.class))
        continue;

      for (Integer p : periods) {

        String key = computeKey(md, f, p);

        CacheManager.getInstance().getCache(CacheManager.MARKETDATA_CACHE).putIfAbsent(new Element(key, new DescriptiveStatistics(p)));

        DescriptiveStatistics ds =
            (DescriptiveStatistics) CacheManager.getInstance().getCache(CacheManager.MARKETDATA_CACHE).get(key).getObjectValue();

        if (f.getFieldType().equals(BigDecimal.class)) {
          ds.addValue(((BigDecimal) f).doubleValue());
        } else {
          ds.addValue((Integer) f.getValue());
        }

        if (ds.getValues().length == p) {

          BaseField<BigDecimal> aveField = new BaseField<BigDecimal>(computeFieldName(md, f, p, "ma"), new BigDecimal(ds.getMean()));

          BaseField<BigDecimal> gmeanField =
              new BaseField<BigDecimal>(computeFieldName(md, f, p, "geo-mean"), new BigDecimal(ds.getGeometricMean()));

          BaseField<BigDecimal> maxField = new BaseField<BigDecimal>(computeFieldName(md, f, p, "max"), new BigDecimal(ds.getMax()));

          BaseField<BigDecimal> minField = new BaseField<BigDecimal>(computeFieldName(md, f, p, "min"), new BigDecimal(ds.getMin()));

          fields.add(aveField);
          fields.add(gmeanField);
          fields.add(maxField);
          fields.add(minField);

        } else {

          BaseField<BigDecimal> field = new BaseField<BigDecimal>(computeFieldName(md, f, p, "ma"), BigDecimal.class);
          fields.add(field);

          field = new BaseField<BigDecimal>(computeFieldName(md, f, p, "geo-mean"), BigDecimal.class);
          fields.add(field);

          field = new BaseField<BigDecimal>(computeFieldName(md, f, p, "max"), BigDecimal.class);
          fields.add(field);

          field = new BaseField<BigDecimal>(computeFieldName(md, f, p, "min"), BigDecimal.class);
          fields.add(field);

        }
      }
    }

    List<Object> values = TupleUtil.propagateTuple(tuple, Lists.newArrayList());
    values.add(fields);

    emit(values.toArray());
    ack();

  }

  @Override
  protected void init() {

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
}
