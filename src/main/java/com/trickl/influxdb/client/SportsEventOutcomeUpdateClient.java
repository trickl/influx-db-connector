package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.SportsEventOutcomeUpdateEntity;
import com.trickl.influxdb.transformers.SportsEventOutcomeUpdateReader;
import com.trickl.influxdb.transformers.SportsEventOutcomeUpdateTransformer;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.pricing.primitives.PriceSource;

import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class SportsEventOutcomeUpdateClient {

  private final InfluxDbClient influxDbClient;

  /**
  * Stores prices in the database.
  *
  * @param priceSource the instrument identifier
  * @param events data to store
  */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventOutcomeUpdate> events) {
    SportsEventOutcomeUpdateTransformer transformer 
        = new SportsEventOutcomeUpdateTransformer(priceSource);
    List<SportsEventOutcomeUpdateEntity> measurements = events.stream().map(transformer)
        .collect(Collectors.toList());
    return influxDbClient.store(measurements,
        CommonDatabases.PRICES.getName(), SportsEventOutcomeUpdateEntity.class,
        SportsEventOutcomeUpdateEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventOutcomeUpdate> findBetween(
      PriceSource priceSource, QueryBetween queryBetween) {
    SportsEventOutcomeUpdateReader reader = new SportsEventOutcomeUpdateReader();
    return influxDbClient.findBetween(
        priceSource, queryBetween,
            CommonDatabases.PRICES.getName(), "sports_event_outcome_update", 
            SportsEventOutcomeUpdateEntity.class)
        .map(reader);
  }

  /**
   * Find all available series that overlap a time window.
   * @param queryBetween A time window there series must have a data point within
   * @return A list of series
   */
  public Flux<PriceSeries> findSeries(QueryBetween queryBetween) {    
    return influxDbClient.findSeries(queryBetween,
        CommonDatabases.PRICES.getName(), "sports_event_outcome_update");
  }
}
