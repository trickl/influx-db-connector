package com.trickl.influxdb.config;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.trickl.influxdb.client.AnalyticPrimitiveValueClient;
import com.trickl.influxdb.client.BrokerOrderClient;
import com.trickl.influxdb.client.CandleClient;
import com.trickl.influxdb.client.CandleStreamClient;
import com.trickl.influxdb.client.InstrumentEventClient;
import com.trickl.influxdb.client.MarketStateChangeClient;
import com.trickl.influxdb.client.OrderBookClient;
import com.trickl.influxdb.client.OrderClient;
import com.trickl.influxdb.client.SportsEventIncidentClient;
import com.trickl.influxdb.client.SportsEventMatchTimeUpdateClient;
import com.trickl.influxdb.client.SportsEventOutcomeUpdateClient;
import com.trickl.influxdb.client.SportsEventPeriodUpdateClient;
import com.trickl.influxdb.client.SportsEventScoreUpdateClient;
import com.trickl.influxdb.client.TransactionClient;
import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxDbConfiguration {

  @Value("${influx-db.url:}")
  private String url;

  @Value("${influx-db.token}")
  private String token;

  @Value("${influx-db.org}")
  private String org;

  @Value("${influx-db.database:prices}")
  private String bucket;

  @Bean
  InfluxDBClientReactive influxDbClient() {
    return InfluxDBClientReactiveFactory.create(url, token.toCharArray(), org, bucket);
  }

  @Bean
  CandleClient influxDbCandleClient() {
    return new CandleClient(influxDbClient(), bucket, org);
  }

  @Bean
  OrderClient influxDbOrderClient() {
    return new OrderClient(influxDbClient(), bucket);
  }

  @Bean
  OrderBookClient influxDbOrderBookClient() {
    return new OrderBookClient(influxDbOrderClient());
  }

  @Bean
  MarketStateChangeClient influxDbMarketStateChangeClient() {
    return new MarketStateChangeClient(influxDbClient(), bucket);
  }

  @Bean
  SportsEventOutcomeUpdateClient influxDbSportsEventOutcomeUpdateClient() {
    return new SportsEventOutcomeUpdateClient(influxDbClient(), bucket);
  }

  @Bean
  SportsEventScoreUpdateClient influxDbSportsEventScoreUpdateClient() {
    return new SportsEventScoreUpdateClient(influxDbClient(), bucket, org);
  }

  @Bean
  SportsEventPeriodUpdateClient influxDbSportsEventPeriodUpdateClient() {
    return new SportsEventPeriodUpdateClient(influxDbClient(), bucket);
  }

  @Bean
  SportsEventMatchTimeUpdateClient influxDbSportsEventMatchTimeUpdateClient() {
    return new SportsEventMatchTimeUpdateClient(influxDbClient(), bucket, org);
  }

  @Bean
  SportsEventIncidentClient influxDbSportsEventIncidentClient() {
    return new SportsEventIncidentClient(influxDbClient(), bucket, org);
  }

  @Bean
  AnalyticPrimitiveValueClient influxDbAnalyticPrimitiveValueClient() {
    return new AnalyticPrimitiveValueClient(influxDbClient(), bucket);
  }

  @Bean
  BrokerOrderClient influxDbBrokerOrderClient() {
    return new BrokerOrderClient(influxDbClient(), bucket);
  }

  @Bean
  TransactionClient influxDbTransactionClient() {
    return new TransactionClient(influxDbClient(), bucket);
  }

  @Bean
  InstrumentEventClient influxDbInstrumentEventClient() {
    return new InstrumentEventClient(
        influxDbMarketStateChangeClient(),
        influxDbSportsEventIncidentClient(),
        influxDbSportsEventOutcomeUpdateClient(),
        influxDbSportsEventScoreUpdateClient(),
        influxDbSportsEventPeriodUpdateClient(),
        influxDbSportsEventMatchTimeUpdateClient());
  }

  @Bean
  CandleStreamClient influxCandleStreamClient() {
    return new CandleStreamClient(influxDbCandleClient(), Instant::now, Duration.ofMinutes(1));
  }
}
