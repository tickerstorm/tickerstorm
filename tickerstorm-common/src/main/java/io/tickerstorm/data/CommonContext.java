package io.tickerstorm.data;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@EnableJms
@Configuration
@PropertySource({"classpath:default.properties"})
public class CommonContext {

  @Value("${jms.transport}")
  protected String transport;

  @Qualifier("realtime")
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildRealtimeEventBus(IPublicationErrorHandler handler) {
    return new MBassador<MarketData>(handler);
  }

  @Qualifier("query")
  @Bean(destroyMethod = "shutdown")
  public MBassador<HistoricalFeedQuery> buildQueryEventBus(IPublicationErrorHandler handler) {
    return new MBassador<HistoricalFeedQuery>(handler);
  }
}
