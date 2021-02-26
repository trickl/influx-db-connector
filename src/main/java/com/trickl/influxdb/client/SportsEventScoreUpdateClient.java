package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.influxdb.transformers.SportsEventScoreUpdateReader;
import com.trickl.influxdb.transformers.SportsEventScoreUpdateTransformer;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class SportsEventScoreUpdateClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventScoreUpdate> events) {
    SportsEventScoreUpdateTransformer transformer =
        new SportsEventScoreUpdateTransformer(priceSource);
    List<SportsEventScoreUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements,
        CommonDatabases.PRICES.getName(),
        SportsEventScoreUpdateEntity.class,
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
    return influxDbClient
        .findBetween(
            priceSource,
            queryBetween,
            CommonDatabases.PRICES.getName(),
            "sports_event_score_update",
            SportsEventScoreUpdateEntity.class)
        .map(reader);
  }

  /**
   * Find a summary of outcome updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(QueryBetween queryBetween) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, CommonDatabases.PRICES.getName(), "sports_event_score_update", "current");
  }
}
