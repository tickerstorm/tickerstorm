package io.tickerstorm.data.service;

import io.tickerstorm.data.MarketDataServiceConfig;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

@Service
@SpringBootApplication
public class MarketDataService {



  public static void main(String[] args) throws Exception {
    SpringApplication.run(MarketDataServiceConfig.class, args);
  }

}
