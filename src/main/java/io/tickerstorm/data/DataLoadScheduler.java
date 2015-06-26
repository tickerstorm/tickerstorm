package io.tickerstorm.data;

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Sets;

@Service
public class DataLoadScheduler {

  private static final Logger logger = LoggerFactory.getLogger(DataLoadScheduler.class);
  private Timer timer;

  private DateTimeZone zone = DateTimeZone.forID("EST");
  private Set<String> symbols = new HashSet<>();
  private Interval period = new Interval(new DateTime().minusYears(1).withZone(zone), new DateTime().withZone(zone));

  @Autowired
  private DataQueryClient client;

  @PostConstruct
  public void init() {
    timer = new Timer(true);
    timer.scheduleAtFixedRate(new StooqTask(), new DateTime().plusSeconds(5).toDate(), 86400000);
    timer.scheduleAtFixedRate(new YahooTask(), new DateTime().plusSeconds(5).toDate(), 86400000);
    timer.scheduleAtFixedRate(new GoogleTask(), new DateTime().plusSeconds(5).toDate(), 86400000);

    symbols.addAll(Sets.newHashSet("ITB", "DHI", "LEN", "PHM", "TOL", "NVR", "HD", "LOW", "TPH", "RYL", "MTH"));
  }

  private class StooqTask extends TimerTask {

    @Override
    public void run() {

//      logger.info("Runnering Stooq data load task");

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

    @Override
    public void run() {

//      logger.info("Runnering Yahoo data load task");
//
//      for (String symbol : symbols) {
//        YahooHistoricalQuoteQuery query = new YahooHistoricalQuoteQuery(symbol).eod().from(period.getStart()).until(period.getEnd());
//        client.query(query);
//
//        YahooChartsDataQuery query2 = new YahooChartsDataQuery(symbol);
//        client.query(query2);
//      }

    }

  }

  private class GoogleTask extends TimerTask {

    @Override
    public void run() {

      logger.info("Runnering Google data load task");

      for (String symbol : symbols) {
        GoogleDataQuery query = new GoogleDataQuery(symbol);
        client.query(query);
      }

    }

  }

}
