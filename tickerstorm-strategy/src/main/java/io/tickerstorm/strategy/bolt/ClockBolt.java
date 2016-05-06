package io.tickerstorm.strategy.bolt;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.strategy.util.Clock;

@Component
@SuppressWarnings("serial")
public class ClockBolt extends BaseBolt {

  @Autowired
  private Clock clock;

  protected void executeMarketData(Tuple tuple) {

    if (tuple.getSourceStreamId().equalsIgnoreCase("default")) {
      MarketData data = (MarketData) tuple.getValueByField(Field.Name.MARKETDATA.field());

      if (data != null) {
        clock.update(data.getTimestamp());
      }
    }

    ack(tuple);

  }
}
