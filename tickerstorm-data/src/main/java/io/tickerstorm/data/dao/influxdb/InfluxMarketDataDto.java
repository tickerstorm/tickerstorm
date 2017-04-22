/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.data.dao.influxdb;

import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field.Name;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Quote;
import io.tickerstorm.common.entity.Tick;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult.Series;

/**
 * Created by kkarski on 4/10/17.
 */
public class InfluxMarketDataDto {

  private Point point;
  private MarketData data;

  InfluxMarketDataDto(MarketData data) {

    this.data = data;

    // Ignore ticks or quotes
    if (Bar.TYPE.equalsIgnoreCase(data.getType())) {

      this.point = Point.measurement("marketdata")
          .time(data.getTimestamp().toEpochMilli(), TimeUnit.MILLISECONDS)
          .tag(Name.SYMBOL.field(), data.getSymbol().toLowerCase())
          .tag(Name.SOURCE.field(), data.getStream().toLowerCase())
          .tag(Name.TYPE.field(), data.getType().toLowerCase())
          .tag(Name.INTERVAL.field(), ((Bar) data).getInterval().toLowerCase())
          .addField(Name.CLOSE.field(), ((Bar) data).close.doubleValue())
          .addField(Name.OPEN.field(), ((Bar) data).getOpen().doubleValue())
          .addField(Name.VOLUME.field(), ((Bar) data).getVolume())
          .addField(Name.HIGH.field(), ((Bar) data).getHigh().doubleValue())
          .addField(Name.LOW.field(), ((Bar) data).getLow().doubleValue())
          .addField(Name.DURATION.field(), InfluxMarketDataDto.toDuration(((Bar) data).getInterval()).get(ChronoUnit.SECONDS))
          .build();

    } else if (Quote.TYPE.equalsIgnoreCase(data.getType())) {

      this.point = Point.measurement("marketdata")
          .time(data.getTimestamp().toEpochMilli(), TimeUnit.MILLISECONDS)
          .tag(Name.SYMBOL.field(), data.getSymbol().toLowerCase())
          .tag(Name.SOURCE.field(), data.getStream().toLowerCase())
          .tag(Name.TYPE.field(), data.getType().toLowerCase())
          .addField(Name.ASK.field(), ((Quote) data).getAsk().doubleValue())
          .addField(Name.ASK_SIZE.field(), ((Quote) data).getAskSize().doubleValue())
          .addField(Name.BID.field(), ((Quote) data).getBid().doubleValue())
          .addField(Name.BID_SIZE.field(), ((Quote) data).getBidSize().doubleValue())
          .build();

    } else if (Tick.TYPE.equalsIgnoreCase(data.getType())) {

      this.point = Point.measurement("marketdata")
          .time(data.getTimestamp().toEpochMilli(), TimeUnit.MILLISECONDS)
          .tag(Name.SYMBOL.field(), data.getSymbol().toLowerCase())
          .tag(Name.SOURCE.field(), data.getStream().toLowerCase())
          .tag(Name.TYPE.field(), data.getType().toLowerCase())
          .addField(Name.PRICE.field(), ((Tick) data).getPrice().doubleValue())
          .addField(Name.QUANTITY.field(), ((Tick) data).getQuantity().doubleValue())
          .build();

    } else {

      throw new IllegalArgumentException("Illegal market data type. Only Bar, Quote and Tick accepted");
    }

  }

  InfluxMarketDataDto(Series series, int i) {

    int symbol = series.getColumns().indexOf(Name.SYMBOL.field());
    int interval = series.getColumns().indexOf(Name.INTERVAL.field());
    int source = series.getColumns().indexOf(Name.SOURCE.field());
    int type = series.getColumns().indexOf(Name.TYPE.field());
    int time = series.getColumns().indexOf("time");
    Instant timestamp = Instant.parse((String) series.getValues().get(i).get(time));
    String typeString = (String) series.getValues().get(i).get(type);

    if (Bar.TYPE.equals(typeString)) {

      int close = series.getColumns().indexOf(Name.CLOSE.field());
      int open = series.getColumns().indexOf(Name.OPEN.field());
      int high = series.getColumns().indexOf(Name.HIGH.field());
      int low = series.getColumns().indexOf(Name.LOW.field());
      int vol = series.getColumns().indexOf(Name.VOLUME.field());

      this.data = new Bar(
          (String) series.getValues().get(i).get(symbol),
          (String) series.getValues().get(i).get(source), timestamp,
          BigDecimal.valueOf((Double) series.getValues().get(i).get(open)),
          BigDecimal.valueOf((Double) series.getValues().get(i).get(close)),
          BigDecimal.valueOf((Double) series.getValues().get(i).get(high)),
          BigDecimal.valueOf((Double) series.getValues().get(i).get(low)),
          (String) series.getValues().get(i).get(interval),
          ((Double) series.getValues().get(i).get(vol)).intValue());

    } else if (Quote.TYPE.equals(typeString)) {

      int ask = series.getColumns().indexOf(Name.ASK.field());
      int askSize = series.getColumns().indexOf(Name.ASK_SIZE.field());
      int bid = series.getColumns().indexOf(Name.BID.field());
      int bidSize = series.getColumns().indexOf(Name.BID_SIZE.field());

      this.data = new Quote((String) series.getValues().get(i).get(symbol),
          (String) series.getValues().get(i).get(source), timestamp,
          BigDecimal.valueOf((Double) series.getValues().get(i).get(ask)),
          (Integer) series.getValues().get(i).get(askSize),
          BigDecimal.valueOf((Double) series.getValues().get(i).get(bid)),
          (Integer) series.getValues().get(i).get(bidSize));

    } else if (Tick.TYPE.equals(typeString)) {

      int price = series.getColumns().indexOf(Name.PRICE.field());
      int quantity = series.getColumns().indexOf(Name.QUANTITY.field());

      this.data = new Tick((String) series.getValues().get(i).get(symbol),
          (String) series.getValues().get(i).get(source), timestamp, BigDecimal.valueOf((Double) series.getValues().get(i).get(price)),
          BigDecimal.valueOf((Double) series.getValues().get(i).get(quantity)));

    }

  }

  public static List<InfluxMarketDataDto> convert(Series series) {

    List<InfluxMarketDataDto> data = new ArrayList<>(series.getValues().size());

    for (int i = 0; i < series.getValues().size(); i++) {
      data.add(new InfluxMarketDataDto(series, i));
    }

    return data;
  }

  public static List<InfluxMarketDataDto> convert(Collection<MarketData> data) {

    return data.stream().map(d -> {
      return new InfluxMarketDataDto(d);
    }).collect(Collectors.toList());

  }

  public static InfluxMarketDataDto convert(MarketData data) {

    return new InfluxMarketDataDto(data);

  }

  public static InfluxMarketDataDto convert(Series series, int i) {
    return new InfluxMarketDataDto(series, i);
  }

  public static Duration toDuration(final String interval) {

    switch (interval.toLowerCase()) {
      case Bar.MIN_1_INTERVAL:
        return Duration.of(60, ChronoUnit.SECONDS);
      case Bar.MIN_5_INTERVAL:
        return Duration.of(300, ChronoUnit.SECONDS);
      case Bar.MIN_10_INTERVAL:
        return Duration.of(600, ChronoUnit.SECONDS);
      case Bar.WEEK_INTERVAL:
        return Duration.of(7, ChronoUnit.DAYS);
    }

    throw new IllegalArgumentException("Unknown duration " + interval);
  }

  protected Point getPoint() {
    return this.point;
  }

  public MarketData toMarketData() {
    return this.data;
  }
}
