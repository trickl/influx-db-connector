package com.trickl.influxdb.config;

import com.trickl.influxdb.client.CandleClient;
import com.trickl.influxdb.client.CandleStreamClient;
import com.trickl.influxdb.client.ConnectionProvider;
import com.trickl.influxdb.client.InfluxDbClient;
import com.trickl.influxdb.client.InstrumentEventClient;
import com.trickl.influxdb.client.MarketStateChangeClient;
import com.trickl.influxdb.client.OrderBookClient;
import com.trickl.influxdb.client.OrderClient;
import com.trickl.influxdb.client.SportsEventIncidentClient;
import com.trickl.influxdb.client.SportsEventOutcomeUpdateClient;
import com.trickl.influxdb.client.SportsEventScoreUpdateClient;
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

  @Bean
  ConnectionProvider connectionProvider() {
    return new ConnectionProvider(url, username, password);
  }

  @Bean
  InfluxDbClient influxDbClient() {
    return new InfluxDbClient(connectionProvider());
  }

  @Bean
  CandleClient influxDbCandleClient() {
    return new CandleClient(influxDbClient());
  }

  @Bean
  OrderClient influxDbOrderClient() {
    return new OrderClient(influxDbClient());
  }

  @Bean
  OrderBookClient influxDbOrderBookClient() {
    return new OrderBookClient(influxDbOrderClient());
  }

  @Bean
  MarketStateChangeClient influxDbMarketStateChangeClient() {
    return new MarketStateChangeClient(influxDbClient());
  }

  @Bean
  SportsEventOutcomeUpdateClient influxDbSportsEventOutcomeUpdateClient() {
    return new SportsEventOutcomeUpdateClient(influxDbClient());
  }

  @Bean
  SportsEventScoreUpdateClient influxDbSportsEventScoreUpdateClient() {
    return new SportsEventScoreUpdateClient(influxDbClient());
  }

  @Bean
  SportsEventIncidentClient influxDbSportsEventIncidentClient() {
    return new SportsEventIncidentClient(influxDbClient());
  }

  @Bean
  InstrumentEventClient influxDbInstrumentEventClient() {
    return new InstrumentEventClient(
        influxDbMarketStateChangeClient(),
        influxDbSportsEventIncidentClient(),
        influxDbSportsEventOutcomeUpdateClient(),
        influxDbSportsEventScoreUpdateClient());
  }

  @Bean
  CandleStreamClient influxCandleStreamClient() {
    return new CandleStreamClient(influxDbCandleClient(), Instant::now, Duration.ofMinutes(1));
  }
}
