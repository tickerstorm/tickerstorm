package io.tickerstorm;

import io.tickerstorm.dao.MarketDataDao;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@ComponentScan(basePackages = { "io.tickerstorm.data" })
@SpringBootApplication
@EnableCassandraRepositories(basePackageClasses = MarketDataDao.class)
@ImportResource(value = { "classpath:/META-INF/spring/cassandra-beans.xml" })
@Import({ CommonConfig.class })
@Configuration
public class DataLoadSchedulerConfig {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(DataLoadSchedulerConfig.class, args);
  }

}
