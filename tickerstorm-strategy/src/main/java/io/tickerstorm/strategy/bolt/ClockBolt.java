package io.tickerstorm.strategy.bolt;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.model.Fields;
import io.tickerstorm.strategy.util.Clock;
import io.tickerstorm.strategy.util.TupleUtil;

@Component
@SuppressWarnings("serial")
public class ClockBolt extends BaseBolt {

  @Autowired
  private Clock clock;

  @Override
  public void process(Tuple tuple) {

    if (tuple.contains(Fields.MARKETDATA.fieldName())) {

      List<Object> values = TupleUtil.propagateTuple(tuple, Lists.newArrayList());

      MarketData data = (MarketData) tuple.getValueByField(Fields.MARKETDATA.fieldName());

      if (data != null) {
        clock.update(data.getTimestamp());
        values.add(clock.now());
        emit(values.toArray());
      }
    }

    ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer arg0) {
    List<String> fields = new ArrayList<String>(Fields.marketdataFields());
    arg0.declare(new backtype.storm.tuple.Fields(fields));
  }
}
