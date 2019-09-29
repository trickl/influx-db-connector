package com.trickl.influxdb.config;

import com.trickl.influxdb.client.CandleClient;
import com.trickl.influxdb.client.OrderBookClient;

import java.util.concurrent.atomic.AtomicReference;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import reactor.core.publisher.Mono;

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
  Mono<InfluxDB> influxDb() {    
    AtomicReference<InfluxDB> connection = new AtomicReference<>();
    return Mono.fromSupplier(() -> connection.updateAndGet((InfluxDB conn) -> {
      if (conn == null) {
        return InfluxDBFactory.connect(url, username, password);
      }
      return conn;
    }))
      .flux()
      .doOnCancel(() -> connection.get().close())
      .publish()
      .single();
  } 
    
  @Bean
  CandleClient influxDbCandleClient() {
    return new CandleClient(influxDb());
  }
  
  @Bean
  OrderBookClient influxDbOrderBookClient() {
    return new OrderBookClient(influxDb(), quoteDepth);
  }
}
