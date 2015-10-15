package io.tickerstorm.client;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import io.tickerstorm.common.data.feed.HistoricalFeedQuery;
import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.StrategyMarker;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@Service
public class BacktestRunnerClient implements ApplicationListener<ContextRefreshedEvent> {

  private static final Logger logger = LoggerFactory.getLogger(BacktestRunnerClient.class);

  @Qualifier("query")
  @Autowired
  private MBassador<HistoricalFeedQuery> queryBus;

  @Qualifier("realtime")
  @Autowired
  private MBassador<MarketData> realtimeBus;


  @PostConstruct
  protected void init() throws Exception {
    realtimeBus.subscribe(this);
  }

  @Handler
  public void onMarketData(MarketData data) {
    logger.debug(data.toString());
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {
    StrategyMarker marker = new StrategyMarker();
    marker.addMarker(Markers.SESSION_START.toString());

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 20, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    logger.info("Dispatching query" + query.toString());
    queryBus.publish(query);

  }


}
