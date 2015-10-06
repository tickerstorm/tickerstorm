package io.tickerstorm.data;

import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@Configuration
@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@PropertySource({"classpath:default.properties"})
public class CommonConfig {

  @Qualifier("historical")
  @Bean
  public MBassador<MarketData> buildEventBus() {
    return new MBassador<MarketData>(new BusConfiguration()
        .addFeature(Feature.SyncPubSub.Default())
        .addFeature(Feature.AsynchronousHandlerInvocation.Default(1, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default()));
  }

  @Qualifier("realtime")
  @Bean
  public MBassador<MarketData> buildRealtimeEventBus() {
    return new MBassador<MarketData>(new BusConfiguration()
        .addFeature(Feature.SyncPubSub.Default())
        .addFeature(Feature.AsynchronousHandlerInvocation.Default(1, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default()));
  }

  @Qualifier("query")
  @Bean
  public MBassador<HistoricalFeedQuery> buildQueryEventBus() {
    return new MBassador<HistoricalFeedQuery>(new BusConfiguration()
        .addFeature(Feature.SyncPubSub.Default())
        .addFeature(Feature.AsynchronousHandlerInvocation.Default(1, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default()));
  }



}
