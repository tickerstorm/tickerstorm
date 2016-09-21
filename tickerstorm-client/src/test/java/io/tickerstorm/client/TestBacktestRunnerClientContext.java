package io.tickerstorm.client;

import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.appx.h2o.H2ORestClient;

@Configuration
@PropertySource({"classpath:default.properties"})
public class TestBacktestRunnerClientContext {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(TestBacktestRunnerClientContext.class);

  @Bean
  public H2ORestClient buildRestClient() throws Exception {
    H2ORestClient client = new H2ORestClient("http://localhost:54321");
    return client;

  }

}
