package io.tickerstorm.client;

import java.io.File;
import java.io.Serializable;
import java.time.Instant;
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
import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketDataMarker;
import io.tickerstorm.common.entity.Notification;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;

@Service
public class BacktestRunnerClient implements ApplicationListener<ContextRefreshedEvent> {

  private static final Logger logger = LoggerFactory.getLogger(BacktestRunnerClient.class);
  private static final String clientName = "TickerStorm client";

  @Qualifier("query")
  @Autowired
  private MBassador<HistoricalFeedQuery> queryBus;

  @Qualifier("commands")
  @Autowired
  private MBassador<Serializable> commandsBus;

  @Qualifier("notification")
  @Autowired
  private MBassador<Serializable> notificationBus;


  @PostConstruct
  protected void init() throws Exception {
    commandsBus.subscribe(this);
    notificationBus.subscribe(this);
  }

  @Handler
  public void onCommandNotification(MarketDataMarker data) throws Exception {

    logger.info("Marker : " + ((MarketDataMarker) data).markers);
    logger.info("Marker : " + ((MarketDataMarker) data).expect);

    if (Markers.is((Marker) data, Markers.QUERY_END) && ((MarketDataMarker) data).expect == 0) {
      Command marker = new Command(clientName, Instant.now());
      marker.addMarker(Markers.SESSION_END.toString());

      Thread.sleep(2000);
      commandsBus.publish(marker);
    }
  }

  @Handler
  public void onCommandNotification(Notification not) throws Exception {

    logger.debug("Client recieved notification " + not.toString());

    if (Markers.is((Marker) not, Markers.CSV_CREATED)) {
      String path = not.getProperties().get("output.file.csv.path");

      assert new File(path).exists() : "File " + path + " doesn't exist";
    }
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent arg0) {

    Command marker = new Command(clientName);
    marker.addMarker(Markers.SESSION_START.toString());
    marker.config.put("output.file.csv.path", "/tmp/MarketDataFile-" + Instant.now() + ".csv");
    commandsBus.publish(marker);

    HistoricalFeedQuery query = new HistoricalFeedQuery("TOL");
    query.from = LocalDateTime.of(2015, 6, 10, 0, 0);
    query.until = LocalDateTime.of(2015, 6, 20, 0, 0);
    query.source = "google";
    query.periods.add(Candle.MIN_1_INTERVAL);
    query.zone = ZoneOffset.ofHours(-7);
    queryBus.publish(query);

  }


}
