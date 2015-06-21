package io.tickerstorm;

import io.tickerstorm.dao.MarketDataDao;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

@ComponentScan(basePackages = { "io.tickerstorm" })
@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = { "classpath:/META-INF/spring/cassandra-beans.xml" })
@Import({ ActiveMQConfig.class })
public class TickerStormConfig {

  @Qualifier("historical")
  @Bean
  public EventBus buildEventBus() {
    return new AsyncEventBus(Executors.newFixedThreadPool(2));
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TickerStormConfig.class, args);
  }

}
