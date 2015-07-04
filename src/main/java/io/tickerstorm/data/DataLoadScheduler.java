package io.tickerstorm.data;

import io.tickerstorm.feed.HistoricalFeedQuery;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DataLoadScheduler {

  private static final Logger logger = LoggerFactory.getLogger(DataLoadScheduler.class);
  private Timer timer;

  @Autowired
  private DataQueryClient client;

  public void start() {

    HistoricalFeedQuery query = new HistoricalFeedQuery("ITB", "DHI", "LEN", "PHM", "TOL", "NVR", "HD", "LOW", "TPH", "RYL", "MTH");

    timer = new Timer(true);
    timer.scheduleAtFixedRate(new StooqTask(query), Date.from(Instant.now().plusSeconds(5)), 86400000);
    timer.scheduleAtFixedRate(new YahooTask(query), Date.from(Instant.now().plusSeconds(5)), 86400000);
    timer.scheduleAtFixedRate(new GoogleTask(query), Date.from(Instant.now().plusSeconds(5)), 86400000);

  }

  private class StooqTask extends TimerTask {

    public StooqTask(HistoricalFeedQuery query) {

    }

    @Override
    public void run() {

      // logger.info("Runnering Stooq data load task");

      // StooqHistoricalForexQuery query = new
      // StooqHistoricalForexQuery().forWorld().currencies().commodities().min5();
      // client.query(query);
      //
      // query = new
      // StooqHistoricalForexQuery().forWorld().currencies().commodities().hourly();
      // client.query(query);
      //
      // query = new
      // StooqHistoricalForexQuery().forWorld().currencies().commodities().daily();
      // client.query(query);
      //
      // query = new StooqHistoricalForexQuery().forUS().stocks().etfs().min5();
      // client.query(query);
      //
      // query = new
      // StooqHistoricalForexQuery().forUS().stocks().etfs().hourly();
      // client.query(query);
      //
      // query = new
      // StooqHistoricalForexQuery().forUS().stocks().etfs().daily();
      // client.query(query);

    }

  }

  private class YahooTask extends TimerTask {

    HistoricalFeedQuery q;

    public YahooTask(HistoricalFeedQuery query) {
      q = query;
    }

    @Override
    public void run() {

      logger.info("Runnering Yahoo data load task");

      for (String symbol : q.symbols) {
        YahooHistoricalQuoteQuery query = new YahooHistoricalQuoteQuery(symbol).eod().from(q.from).until(q.until);
        client.query(query);

        YahooChartsDataQuery query2 = new YahooChartsDataQuery(symbol);
        client.query(query2);
      }

    }

  }

  private class GoogleTask extends TimerTask {

    HistoricalFeedQuery q;

    public GoogleTask(HistoricalFeedQuery query) {
      q = query;
    }

    @Override
    public void run() {

      logger.info("Runnering Google data load task");

      Duration p = Duration.between(q.from, q.until);

      for (String symbol : q.symbols) {
        GoogleDataQuery query = new GoogleDataQuery(symbol).days((int) p.get(ChronoUnit.DAYS));
        client.query(query);
      }

    }

  }

}
