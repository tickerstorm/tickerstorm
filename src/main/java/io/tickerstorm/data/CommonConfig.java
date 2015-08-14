package io.tickerstorm.data;

import io.tickerstorm.data.dao.MarketDataDao;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

@Configuration
@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@ComponentScan(basePackages = {"io.tickerstorm.data"})
@PropertySource({"classpath:default.properties"})
public class CommonConfig {

  @Bean
  public PropertySourcesPlaceholderConfigurer getConfigured() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  @Qualifier("busExecutor")
  @Bean
  public Executor buildThreadPool() {
    return Executors.newFixedThreadPool(2);
  }

  @Qualifier("historical")
  @Bean
  public EventBus buildEventBus(@Qualifier("busExecutor") Executor exec) {
    return new AsyncEventBus(exec);
  }

  @Qualifier("realtime")
  @Bean
  public EventBus buildRealtimeEventBus(@Qualifier("busExecutor") Executor exec) {
    return new AsyncEventBus(exec);
  }

  @Qualifier("query")
  @Bean
  public EventBus buildQueryEventBus(@Qualifier("busExecutor") Executor exec) {
    return new AsyncEventBus(exec);
  }



}
