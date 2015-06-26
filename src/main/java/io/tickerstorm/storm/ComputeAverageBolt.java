package io.tickerstorm.storm;

import io.tickerstorm.entity.Candle;

import java.util.Map;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;

import eu.verdelhan.ta4j.Decimal;
import eu.verdelhan.ta4j.Tick;
import eu.verdelhan.ta4j.TimeSeries;
import eu.verdelhan.ta4j.indicators.simple.ClosePriceIndicator;
import eu.verdelhan.ta4j.indicators.trackers.EMAIndicator;
import eu.verdelhan.ta4j.indicators.trackers.SMAIndicator;

@Component
@SuppressWarnings("serial")
public class ComputeAverageBolt extends BaseRichBolt {

  private final static Logger logger = LoggerFactory.getLogger(ComputeAverageBolt.class);
  private OutputCollector coll;

  @Autowired
  private CacheManager cacheManager;

  @Override
  public void execute(Tuple tuple) {

    Candle candle = (Candle) tuple.getValueByField(Fields.CANDEL);
    TimeSeries series = null;

    if (candle != null) {

      Tick tick = new Tick(candle.getPeriod(), candle.getTimestamp(), Decimal.valueOf(candle.open.toPlainString()),
          Decimal.valueOf(candle.high.toPlainString()), Decimal.valueOf(candle.low.toPlainString()), Decimal.valueOf(candle.close
              .toPlainString()), Decimal.valueOf(candle.volume.toPlainString()));

      String key = new StringBuffer(candle.symbol).append("-").append(candle.interval).append("-").append("-timeseries").toString();

      if (!cacheManager.getCache("timeseries").isKeyInCache(key)) {

        series = new TimeSeries(candle.getPeriod());
        series.addTick(tick);
        cacheManager.getCache("timeseries").putIfAbsent(new Element(key, series));

      } else {

        series = ((TimeSeries) cacheManager.getCache("timeseries").get(key).getObjectValue());
        series.addTick(tick);

      }
    }

    TimeSeries days30 = series.subseries(0, Period.days(30));
    int lastTick = days30.getEnd();
    ClosePriceIndicator closeInd = new ClosePriceIndicator(days30);
    SMAIndicator sma = new SMAIndicator(closeInd, lastTick);
    EMAIndicator ema = new EMAIndicator(closeInd, lastTick);

    coll.emit(tuple, Lists.newArrayList(sma.getValue(lastTick), ema.getValue(lastTick)));
    coll.ack(tuple);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer dec) {
    dec.declare(new backtype.storm.tuple.Fields("sma", "ema"));
  }

  @Override
  public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
    // TODO Auto-generated method stub

  }

}
