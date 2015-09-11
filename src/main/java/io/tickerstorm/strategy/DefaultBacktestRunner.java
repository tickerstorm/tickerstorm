package io.tickerstorm.strategy;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.strategy.backtest.BacktestRunner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@Component
@SpringBootApplication
public class DefaultBacktestRunner extends BacktestRunner {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(BacktestConfig.class, args);
  }

  @Override
  protected void initData() {

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 20, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    sendQuery(query);

  }

}
