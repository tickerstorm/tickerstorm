package io.tickerstorm.messaging;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.activemq.broker.BrokerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MessagingService {

  @Autowired
  private BrokerService broker;

  @PostConstruct
  public void init() throws Exception {
    broker.start();

  }

  @PreDestroy
  public void destory() throws Exception {
    broker.stop();
  }

}
