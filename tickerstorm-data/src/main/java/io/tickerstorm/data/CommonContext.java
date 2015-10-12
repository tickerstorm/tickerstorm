package io.tickerstorm.data;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.JmsTemplate;

import io.tickerstorm.data.feed.HistoricalFeedQuery;
import io.tickerstorm.data.jms.ByDestinationNameJmsResolver;
import io.tickerstorm.data.jms.Destinations;
import io.tickerstorm.data.jms.EventBusToJMSBridge;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.common.Properties;
import net.engio.mbassy.bus.config.BusConfiguration;
import net.engio.mbassy.bus.config.Feature;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@EnableJms
@Configuration
@PropertySource({"classpath:default.properties"})
public class CommonContext {

  @Value("${jms.transport}")
  protected String transport;

  @Qualifier("historical")
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildEventBus(IPublicationErrorHandler handler) {
    return new MBassador<MarketData>(new BusConfiguration().addFeature(Feature.SyncPubSub.Default())
        .addFeature(Feature.AsynchronousHandlerInvocation.Default(1, 4))
        .addFeature(Feature.AsynchronousMessageDispatch.Default())
        .addPublicationErrorHandler(handler).setProperty(Properties.Common.Id, "historical bus"));
  }

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
  public EventBusToJMSBridge buildRealtimeJmsBridge(
      @Qualifier("realtime") MBassador<MarketData> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template);
  }

  @Bean
  public EventBusToJMSBridge buildQueryJmsBridge(
      @Qualifier("query") MBassador<HistoricalFeedQuery> eventbus, JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_QUERY, template);
  }

  @Bean
  public JmsTemplate buildJmsTemplate(ConnectionFactory factory) {
    JmsTemplate template = new JmsTemplate(factory);
    template.setDestinationResolver(new ByDestinationNameJmsResolver());
    template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    template.setTimeToLive(2000);
    return template;
  }


}
