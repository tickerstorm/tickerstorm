package io.tickerstorm.strategy.bolt;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;

@SuppressWarnings("serial")
public class FieldTypeSplittingBolt extends BaseBolt {

  @Override
  protected void executeMarketData(Tuple tuple) {

    MarketData md = (MarketData) tuple.getValueByField(Field.Name.MARKETDATA.field());

    coll.emit(tuple, new Values(md));
    coll.emit(Field.Name.CONTINOUS_FIELDS.field(), tuple, new Values(filterForFields(md, BigDecimal.class)));
    coll.emit(Field.Name.DISCRETE_FIELDS.field(), tuple, new Values(filterForFields(md, Integer.class)));
    coll.emit(Field.Name.CATEGORICAL_FIELDS.field(), tuple, new Values(filterForFields(md, String.class)));
    coll.emit(Field.Name.TEMPORAL_FIELDS.field(), tuple, new Values(filterForFields(md, Instant.class)));
    ack();

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Field.Name.MARKETDATA.field()));
    declarer.declareStream(Field.Name.CONTINOUS_FIELDS.field(), new Fields(Field.Name.CONTINOUS_FIELDS.field()));
    declarer.declareStream(Field.Name.DISCRETE_FIELDS.field(), new Fields(Field.Name.DISCRETE_FIELDS.field()));
    declarer.declareStream(Field.Name.CATEGORICAL_FIELDS.field(), new Fields(Field.Name.CATEGORICAL_FIELDS.field()));
    declarer.declareStream(Field.Name.TEMPORAL_FIELDS.field(), new Fields(Field.Name.TEMPORAL_FIELDS.field()));
  }

  protected Set<Field<?>> filterForFields(MarketData md, Class<?> clazz) {

    HashSet<Field<?>> insts = new HashSet<>();

    for (Field<?> f : md.getFields()) {

      if (f.getFieldType().isAssignableFrom(clazz)) {
        insts.add((Field<?>) f);
      }
    }

    return insts;

  }
}
