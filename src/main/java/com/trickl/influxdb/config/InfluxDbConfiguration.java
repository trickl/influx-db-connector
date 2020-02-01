package com.trickl.influxdb.config;

import com.trickl.influxdb.client.CandleClient;
import com.trickl.influxdb.client.CandleStreamClient;
import com.trickl.influxdb.client.InfluxDbClient;
import com.trickl.influxdb.client.OrderBookClient;

import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class InfluxDbConfiguration {
    
  @Value("${influx-db.url:}")
  private String url;  
    
  @Value("${influx-db.username:root}")
  private String username; 
  
  @Value("${influx-db.password:root}")
  private String password; 

  @Value("${influx-db.quote-depth:3}")
  private int quoteDepth; 

  @Bean
  InfluxDbClient influxDbClient() {    
    return new InfluxDbClient(url, username, password);
  }
  
    
  @Bean
  CandleClient influxDbCandleClient() {
    return new CandleClient(influxDbClient());
  }
  
  @Bean
  OrderBookClient influxDbOrderBookClient() {
    return new OrderBookClient(influxDbClient(), quoteDepth);
  }

  @Bean
  CandleStreamClient influxCandleStreamClient() {
    return new CandleStreamClient(
        influxDbCandleClient(),
        Instant::now,
        Duration.ofMinutes(1));
  }
}
