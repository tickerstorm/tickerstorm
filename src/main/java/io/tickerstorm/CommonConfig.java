package io.tickerstorm;

import io.tickerstorm.dao.MarketDataDao;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = { "classpath:/META-INF/spring/cassandra-beans.xml" })
@ComponentScan(basePackages = { "io.tickerstorm.dao", "io.tickerstorm.entity" })
@Configuration
@PropertySource({ "classpath:default.properties" })
public class CommonConfig {

  @Qualifier("historical")
  @Bean
  public EventBus buildEventBus() {
    return new AsyncEventBus(Executors.newFixedThreadPool(2));
  }

  @Qualifier("realtime")
  @Bean
  public EventBus buildRealtimeEventBus() {
    return new AsyncEventBus(Executors.newFixedThreadPool(2));
  }

}
