package io.tickerstorm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@ComponentScan(basePackages = { "io.tickerstorm" })
@SpringBootApplication
@Import({ ActiveMQConfig.class, CommonConfig.class })
public class TickerStormConfig {

  public static void main(String[] args) throws Exception {
    SpringApplication.run(TickerStormConfig.class, args);
  }

}
