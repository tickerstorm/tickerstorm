package io.tickerstorm.strategy;

import java.net.URI;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.common.data.eventbus.Destinations;
import io.tickerstorm.common.data.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.data.eventbus.JMSToEventBusBridge;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

@EnableAutoConfiguration
@Configuration
public class StrategyServiceTestApplication extends StrategyServiceApplication {

  @Value("${jms.transport}")
  protected String transport;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(StrategyServiceTestApplication.class, args);
  }

  @PostConstruct
  public void init() {
    //startActiveMQ();
  }


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

  // SENDERS
  @Bean
  public EventBusToJMSBridge buildRealtimeJmsBridge(@Qualifier("realtime") MBassador<MarketData> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template);
  }

  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier("modelData") MBassador<Map<String, Object>> modelDataBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge();
    bridge.setModelDataBus(modelDataBus);
    return bridge;
  }

}
