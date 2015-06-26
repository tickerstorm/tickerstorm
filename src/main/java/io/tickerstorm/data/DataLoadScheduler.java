package io.tickerstorm.data;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;

import org.joda.time.DateTime;
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

  @PostConstruct
  public void init() {
    timer = new Timer(true);
    timer.scheduleAtFixedRate(new StooqTask(), new DateTime().plusSeconds(15).toDate(), 86400000);
    timer.scheduleAtFixedRate(new YahooTask(), new DateTime().plusSeconds(15).toDate(), 86400000);
  }

  private class StooqTask extends TimerTask {

    @Override
    public void run() {

      logger.info("Runnering Stooq data load task");

//      StooqHistoricalForexQuery query = new StooqHistoricalForexQuery().forWorld().currencies().commodities().min5();
//      client.query(query);
//
//      query = new StooqHistoricalForexQuery().forWorld().currencies().commodities().hourly();
//      client.query(query);
//
//      query = new StooqHistoricalForexQuery().forWorld().currencies().commodities().daily();
//      client.query(query);
//
//      query = new StooqHistoricalForexQuery().forUS().stocks().etfs().min5();
//      client.query(query);
//
//      query = new StooqHistoricalForexQuery().forUS().stocks().etfs().hourly();
//      client.query(query);
//
//      query = new StooqHistoricalForexQuery().forUS().stocks().etfs().daily();
//      client.query(query);

    }

  }

  private class YahooTask extends TimerTask {

    @Override
    public void run() {

      logger.info("Runnering Yahoo data load task");

      YahooHistoricalQuoteQuery query = new YahooHistoricalQuoteQuery("AAPL").eod();
      client.query(query);
      
      YahooChartsDataQuery query2 = new YahooChartsDataQuery("AAPL");
      client.query(query2);

    }

  }

}
