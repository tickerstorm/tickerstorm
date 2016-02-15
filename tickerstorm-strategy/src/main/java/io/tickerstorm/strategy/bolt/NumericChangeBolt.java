package io.tickerstorm.strategy.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.Field;

@SuppressWarnings("serial")
public class NumericChangeBolt extends BaseBolt {

  @Override
  protected void process(Tuple input) {

    // if (input.getSourceStreamId().equalsIgnoreCase(Field.Name.CONTINOUS_FIELDS.field())) {
    //
    // Set<Field<BigDecimal>> fields = (Set<Field<BigDecimal>>)
    // input.getValueByField(Field.Name.CONTINOUS_FIELDS.field());
    //
    // for (Field<BigDecimal> f : fields) {
    //
    // Candle previous = (Candle) CacheManager.getInstance("change-cache").get(key);
    //
    // // We are going in reverse time, the previous tuple happens after the current in timeseries
    // if (md.getTimestamp().isBefore(previous.getTimestamp())) {
    //
    // BigDecimal chClose = ((Candle) md).close.subtract(previous.close).divide(previous.close);
    //
    // }
    //
    // }
    // }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new backtype.storm.tuple.Fields(Field.Name.CHANGE.field()));
  }

}
