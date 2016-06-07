package io.tickerstorm.client;

import java.io.Serializable;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.appx.h2o.H2ORestClient;

import io.tickerstorm.common.data.eventbus.MBassadorErrorHandler;
import io.tickerstorm.common.data.query.DataFeedQuery;
import io.tickerstorm.common.entity.MarketData;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.error.IPublicationErrorHandler;

@Configuration
@PropertySource({"classpath:default.properties"})
public class TestBacktestRunnerClientContext {

  public static final Logger logger = org.slf4j.LoggerFactory.getLogger(TestBacktestRunnerClientContext.class);

  @Bean
  public IPublicationErrorHandler buildErrorHandler() {
    return new MBassadorErrorHandler();
  }

  @Qualifier("realtime")
  @Bean(destroyMethod = "shutdown")
  public MBassador<MarketData> buildRealtimeEventBus(IPublicationErrorHandler handler) {
    return new MBassador<MarketData>(handler);
  }

  @Qualifier("query")
  @Bean(destroyMethod = "shutdown")
  public MBassador<DataFeedQuery> buildQueryEventBus(IPublicationErrorHandler handler) {
    return new MBassador<DataFeedQuery>(handler);
  }

  @Qualifier("commands")
  @Bean(destroyMethod = "shutdown")
  public MBassador<Serializable> buildCommandsEventBus(IPublicationErrorHandler handler) {
    return new MBassador<Serializable>(handler);
  }

  @Qualifier("notification")
  @Bean(destroyMethod = "shutdown")
  public MBassador<Serializable> buildNotificaitonEventBus(IPublicationErrorHandler handler) {
    return new MBassador<Serializable>(handler);
  }

  @Bean
  public H2ORestClient buildRestClient() throws Exception {
    H2ORestClient client = new H2ORestClient("http://localhost:54321");
    return client;

  }

}
