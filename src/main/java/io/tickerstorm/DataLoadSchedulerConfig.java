package io.tickerstorm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@ComponentScan(basePackages = { "io.tickerstorm.data" })
@PropertySource({ "classpath:default.properties" })
@Import({ CommonConfig.class })
public class DataLoadSchedulerConfig {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(DataLoadSchedulerConfig.class, args);
  }

}
