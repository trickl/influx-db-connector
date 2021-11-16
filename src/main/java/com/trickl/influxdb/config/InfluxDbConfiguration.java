package com.trickl.influxdb.config;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.trickl.influxdb.client.AnalyticDoubleValueClient;
import com.trickl.influxdb.client.CandleClient;
import com.trickl.influxdb.client.CandleStreamClient;
import com.trickl.influxdb.client.InfluxDbAdapter;
import com.trickl.influxdb.client.InfluxDbAggregator;
import com.trickl.influxdb.client.InstrumentEventClient;
import com.trickl.influxdb.client.MarketStateChangeClient;
import com.trickl.influxdb.client.OrderBookClient;
import com.trickl.influxdb.client.OrderClient;
import com.trickl.influxdb.client.SportsEventIncidentClient;
import com.trickl.influxdb.client.SportsEventMatchTimeUpdateClient;
import com.trickl.influxdb.client.SportsEventOutcomeUpdateClient;
import com.trickl.influxdb.client.SportsEventPeriodUpdateClient;
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
  InfluxDbAdapter influxDbAdapter() {
    return new InfluxDbAdapter(influxDbClient(), bucket);
  }

  @Bean
  InfluxDbAggregator influxDbAggregator() {
    return new InfluxDbAggregator(influxDbClient(), bucket, org);
  }

  @Bean
  CandleClient influxDbCandleClient() {
    return new CandleClient(influxDbAdapter(), influxDbAggregator());
  }

  @Bean
  OrderClient influxDbOrderClient() {
    return new OrderClient(influxDbAdapter());
  }

  @Bean
  OrderBookClient influxDbOrderBookClient() {
    return new OrderBookClient(influxDbOrderClient());
  }

  @Bean
  MarketStateChangeClient influxDbMarketStateChangeClient() {
    return new MarketStateChangeClient(influxDbAdapter());
  }

  @Bean
  SportsEventOutcomeUpdateClient influxDbSportsEventOutcomeUpdateClient() {
    return new SportsEventOutcomeUpdateClient(influxDbAdapter());
  }

  @Bean
  SportsEventScoreUpdateClient influxDbSportsEventScoreUpdateClient() {
    return new SportsEventScoreUpdateClient(influxDbAdapter(), influxDbAggregator());
  }

  @Bean
  SportsEventPeriodUpdateClient influxDbSportsEventPeriodUpdateClient() {
    return new SportsEventPeriodUpdateClient(influxDbAdapter());
  }

  @Bean
  SportsEventMatchTimeUpdateClient influxDbSportsEventMatchTimeUpdateClient() {
    return new SportsEventMatchTimeUpdateClient(influxDbAdapter(), influxDbAggregator());
  }

  @Bean
  SportsEventIncidentClient influxDbSportsEventIncidentClient() {
    return new SportsEventIncidentClient(influxDbAdapter(), influxDbAggregator());
  }

  @Bean
  AnalyticDoubleValueClient influxDbAnalyticDoubleValueClient() {
    return new AnalyticDoubleValueClient(influxDbAdapter());
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
