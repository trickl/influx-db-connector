package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.influxdb.transformers.SportsEventScoreUpdateReader;
import com.trickl.influxdb.transformers.SportsEventScoreUpdateTransformer;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.pricing.primitives.PriceSource;

import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class SportsEventScoreUpdateClient {

  private final InfluxDbClient influxDbClient;

  /**
  * Stores prices in the database.
  *
  * @param priceSource the instrument identifier
  * @param events data to store
  */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventScoreUpdate> events) {
    SportsEventScoreUpdateTransformer transformer 
        = new SportsEventScoreUpdateTransformer(priceSource);
    List<SportsEventScoreUpdateEntity> measurements = events.stream().map(transformer)
        .collect(Collectors.toList());
    return influxDbClient.store(measurements, "prices", SportsEventScoreUpdateEntity.class,
        SportsEventScoreUpdateEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventScoreUpdate> findBetween(
       PriceSource priceSource, QueryBetween queryBetween) {
    SportsEventScoreUpdateReader reader = new SportsEventScoreUpdateReader();
    return influxDbClient.findBetween(
        priceSource, queryBetween, "prices", "sports_event_score_update",
        SportsEventScoreUpdateEntity.class)
        .map(reader);
  }
}