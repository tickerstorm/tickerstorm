package io.tickerstorm;

import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

import io.tickerstorm.common.data.CommonContext;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import io.tickerstorm.strategy.StrategyServiceTestApplication;

@EnableAutoConfiguration
@Configuration
@PropertySource({"classpath:default.properties"})
@Import({StrategyServiceTestApplication.class, TestMarketDataServiceConfig.class, CommonContext.class})
public class IntegrationTestContext {

  @Value("${jms.transport}")
  protected String transport;

  @Profile("embedded")
  @Bean(initMethod = "start", destroyMethod = "stop")
  public BrokerService startActiveMQ() {

    BrokerService broker = null;

    try {
      broker = new BrokerService();
      broker.setBrokerName("tickerstorm");
      TransportConnector connector = new TransportConnector();
      connector.setUri(new URI(transport));
      broker.addConnector(connector);
      broker.setPersistent(false);
    } catch (Throwable e) {
      // nothing
    }
    return broker;
  }

}
