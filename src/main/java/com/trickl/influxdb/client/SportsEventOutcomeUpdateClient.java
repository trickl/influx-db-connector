package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.SportsEventOutcomeUpdateReader;
import com.trickl.influxdb.binding.SportsEventOutcomeUpdateWriter;
import com.trickl.influxdb.persistence.SportsEventOutcomeUpdateEntity;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SportsEventOutcomeUpdateClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return the number of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventOutcomeUpdate> events) {
    SportsEventOutcomeUpdateWriter transformer = new SportsEventOutcomeUpdateWriter(priceSource);
    List<SportsEventOutcomeUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
        measurements,
        SportsEventOutcomeUpdateEntity.class,
        SportsEventOutcomeUpdateEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventOutcomeUpdate> findBetween(
      EventSource eventSource, QueryBetween queryBetween) {
    SportsEventOutcomeUpdateReader reader = new SportsEventOutcomeUpdateReader();
    if (eventSource.getEventSubType() != null) {
      // Sub-types not supported
      return Flux.empty();
    }
    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            "sports_event_outcome_update",
            SportsEventOutcomeUpdateEntity.class)
        .map(reader);
  }

  /**
   * Find a summary of outcome updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration influxDbClient =
        new InfluxDbFirstLastDuration(this.influxDbClient, bucket);
    return influxDbClient.firstLastDuration(
        queryBetween, "sports_event_outcome_update", "outcome", priceSource);
  }

  /**
   * Find a count of outcome updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbClient = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbClient
        .count(queryBetween, "sports_event_outcome_update", "outcome", priceSource)
        .map(PriceSourceInteger::getValue);
  }
}
