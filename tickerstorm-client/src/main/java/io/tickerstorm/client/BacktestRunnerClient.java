package io.tickerstorm.client;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.entity.Candle;
import io.tickerstorm.entity.Markers;
import io.tickerstorm.entity.MarketData;
import io.tickerstorm.entity.StrategyMarker;
import net.engio.mbassy.bus.MBassador;

@Component
@SpringBootApplication
public class BacktestRunnerClient {

  @Qualifier("query")
  @Autowired
  private MBassador<HistoricalFeedQuery> queryBus;

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(BacktestRunnerClientContext.class, args);
  }

  @PostConstruct
  protected void init() {

    StrategyMarker marker = new StrategyMarker();
    marker.addMarker(Markers.SESSION_START.toString());

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 20, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    queryBus.publish(query);


  }


}
