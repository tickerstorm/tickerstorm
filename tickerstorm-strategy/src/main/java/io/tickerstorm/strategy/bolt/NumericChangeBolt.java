package io.tickerstorm.strategy.bolt;


import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

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
    declarer.declare(new Fields(Field.Name.CHANGE.field()));
  }

}
