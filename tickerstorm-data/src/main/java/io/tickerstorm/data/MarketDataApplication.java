/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.data;

import com.google.common.eventbus.EventBus;
import io.tickerstorm.common.EventBusContext;
import io.tickerstorm.common.JmsEventBusContext;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.eventbus.EventBusToEventBusBridge;
import io.tickerstorm.common.eventbus.EventBusToJMSBridge;
import io.tickerstorm.common.eventbus.JMSToEventBusBridge;
import io.tickerstorm.data.dao.MarketDataDao;
import io.tickerstorm.data.dao.ModelDataDao;
import io.tickerstorm.service.HeartBeatGenerator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.jms.core.JmsTemplate;

@SpringBootApplication(scanBasePackages = {"io.tickerstorm.data"})
@EnableCassandraRepositories(basePackageClasses = {MarketDataDao.class, ModelDataDao.class})
@ImportResource(value = {"classpath:/META-INF/spring/cassandra-beans.xml"})
@PropertySource({"classpath:/default.properties"})
@Import({EventBusContext.class, JmsEventBusContext.class})
public class MarketDataApplication {

  @Value("${service.name:data-service}")
  private String SERVICE;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(MarketDataApplication.class, args);
  }

  @Bean
  public HeartBeatGenerator generateDataServiceHeartBeat() {
    return new HeartBeatGenerator(SERVICE, 5000);
  }

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Bean
  public EventBus buildHistoricalEventBus() {
    // return new AsyncEventBus(Destinations.HISTORICL_MARKETDATA_BUS, executor);
    return new EventBus(Destinations.HISTORICL_MARKETDATA_BUS);

  }

  // SENDERS
  @Qualifier(Destinations.REALTIME_MARKETDATA_BUS)
  @Bean
  public EventBusToJMSBridge buildRealtimeJmsBridge(@Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_REALTIME_MARKETDATA, template, SERVICE);
  }

  @Qualifier(Destinations.RETRO_MODEL_DATA_BUS)
  @Bean
  public EventBusToJMSBridge buildRetroModelDataJmsBridge(@Qualifier(Destinations.RETRO_MODEL_DATA_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.QUEUE_RETRO_MODEL_DATA, template, SERVICE);
  }

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Bean
  public EventBusToJMSBridge buildNotificationsJmsBridge(@Qualifier(Destinations.NOTIFICATIONS_BUS) EventBus eventbus,
      JmsTemplate template) {
    return new EventBusToJMSBridge(eventbus, Destinations.TOPIC_NOTIFICATIONS, template, SERVICE, 5000);
  }

  /**
   * Enable market realtime data from broker to be directly streamed to internal realtime market
   * data bus so that models can act upon live data
   * 
   * @param source
   * @param listener
   * @return
   */
  @Qualifier(Destinations.BROKER_MARKETDATA_BUS)
  @Bean
  public EventBusToEventBusBridge<MarketData> buildBrokerFeedEventBridge(@Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus source,
      @Qualifier(Destinations.REALTIME_MARKETDATA_BUS) EventBus listener,
      @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS) EventBus historical) {
    EventBusToEventBusBridge<MarketData> bridge = new EventBusToEventBusBridge<MarketData>(source, listener);
    bridge.addListener(historical);// subscribe to broker data to record
    return bridge;
  }

  // RECEIVERS
  @Bean
  public JMSToEventBusBridge buildQueryEventBridge(@Qualifier(Destinations.MODEL_DATA_BUS) EventBus modelDataBus,
      @Qualifier(Destinations.COMMANDS_BUS) EventBus commandBus, @Qualifier(Destinations.BROKER_MARKETDATA_BUS) EventBus brokerFeedBus) {
    JMSToEventBusBridge bridge = new JMSToEventBusBridge(SERVICE);
    bridge.modelDataBus = modelDataBus;
    bridge.commandsBus = commandBus;
    bridge.brokerFeedBus = brokerFeedBus;
    return bridge;
  }
}
