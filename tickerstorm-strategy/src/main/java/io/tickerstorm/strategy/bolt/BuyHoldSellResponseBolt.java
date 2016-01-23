package io.tickerstorm.strategy.bolt;

import java.math.BigDecimal;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.model.Fields;
import io.tickerstorm.strategy.util.CacheManager;

@SuppressWarnings("serial")
public class BuyHoldSellResponseBolt extends BaseBolt {

  @Override
  protected void process(Tuple input) {

    if (input.contains(Fields.MARKETDATA.toString()) && input.contains(Fields.NOW.toString())) {

      MarketData md = (MarketData) input.getValueByField(Fields.MARKETDATA.toString());

      if (md.getType().equals(Candle.TYPE)) {

        Candle previous = (Candle) CacheManager.getInstance().getCache(CacheManager.MARKETDATA_CACHE).get(Fields.BUY_HOLD_SELL.toString())
            .getObjectValue();

        // We are going in reverse time, the previous tuple happens after the current in timeseries
        if (md.getTimestamp().isBefore(previous.getTimestamp())) {

          BigDecimal chClose = ((Candle) md).close.divide(previous.close);
          BigDecimal chOpen = ((Candle) md).open.divide(previous.open);
          BigDecimal chHigh = ((Candle) md).high.divide(previous.high);
          BigDecimal chLow = ((Candle) md).low.divide(previous.low);

          int chCloseDir = chClose.compareTo(BigDecimal.ONE);
          int chOpenDir = chOpen.compareTo(BigDecimal.ONE);
          int chHighDir = chHigh.compareTo(BigDecimal.ONE);
          int chLowDir = chLow.compareTo(BigDecimal.ONE);

          if (chClose.compareTo(new BigDecimal("1.001")) > 1 || chClose.compareTo(new BigDecimal("0.009")) > 1)
            chCloseDir = 0;
        }
      }
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new backtype.storm.tuple.Fields(Fields.AVE.toString()));
  }

}
