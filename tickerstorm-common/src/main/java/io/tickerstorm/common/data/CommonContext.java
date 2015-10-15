package io.tickerstorm.common.data;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

import io.tickerstorm.common.data.eventbus.ByDestinationNameJmsResolver;
import io.tickerstorm.common.data.feed.HistoricalFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@EnableJms
@Configuration
@ComponentScan("io.tickerstorm.common")
@PropertySource({"classpath:default.properties"})
public class CommonContext {

  @Value("${jms.transport}")
  protected String transport;

  @Qualifier("realtime")
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildRealtimeEventBus(IPublicationErrorHandler handler) {
    return new MBassador<MarketData>(handler);
  }

  @Qualifier("query")
  @Bean(destroyMethod = "shutdown")
  public MBassador<HistoricalFeedQuery> buildQueryEventBus(IPublicationErrorHandler handler) {
    return new MBassador<HistoricalFeedQuery>(handler);
  }

  @Bean
  public ConnectionFactory buildActiveMQConnectionFactory() throws Exception {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(transport);
    return connectionFactory;
  }

  @Bean
  public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory cf) {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    factory.setConnectionFactory(cf);
    factory.setDestinationResolver(new ByDestinationNameJmsResolver());
    factory.setConcurrency("3-10");
    factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    return factory;
  }
}
