package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.AggregatedSportsEventScoreUpdateEntity;
import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.influxdb.transformers.AggregatedSportsEventScoreUpdateReader;
import com.trickl.influxdb.transformers.SportsEventScoreUpdateReader;
import com.trickl.influxdb.transformers.SportsEventScoreUpdateTransformer;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class SportsEventScoreUpdateClient {

  private final InfluxDbAdapter influxDbClient;

  private final InfluxDbAggregator influxDbAggregator;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return the number of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventScoreUpdate> events) {
    SportsEventScoreUpdateTransformer transformer =
        new SportsEventScoreUpdateTransformer(priceSource);
    List<SportsEventScoreUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements, SportsEventScoreUpdateEntity.class, SportsEventScoreUpdateEntity::getTime);
  }

  /**
   * Find sports updates.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventScoreUpdate> findBetween(
      EventSource eventSource, QueryBetween queryBetween) {
    SportsEventScoreUpdateReader reader = new SportsEventScoreUpdateReader();
    if (eventSource.getEventSubType() != null) {
      // Sub-types not supported
      return Flux.empty();
    }
    return influxDbClient
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            "sports_event_score_update",
            SportsEventScoreUpdateEntity.class)
        .map(reader);
  }

  /**
   * Find aggregated sports updates.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<AggregatedInstrumentEvents> findAggregatedBetween(
      EventSource eventSource, QueryBetween queryBetween) {
    AggregatedSportsEventScoreUpdateReader reader = new AggregatedSportsEventScoreUpdateReader();

    return influxDbClient
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            eventSource.getEventType(),
            AggregatedSportsEventScoreUpdateEntity.class,
            Optional.empty())
        .map(reader);
  }

  /**
   * Aggregate sports updates.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @param aggregateEventWidth the period of an aggregation window
   * @return A list of bars
   */
  public Flux<AggregatedInstrumentEvents> aggregateBetween(
      EventSource eventSource, QueryBetween queryBetween, Duration aggregateEventWidth) {
    AggregatedSportsEventScoreUpdateReader reader = new AggregatedSportsEventScoreUpdateReader();
    String measurementName = eventSource.getEventType();    
    return influxDbAggregator
        .aggregateSportsEventScoreUpdatesBetween(
            eventSource.getPriceSource(),
            queryBetween,
            measurementName,
            aggregateEventWidth,
            Optional.empty())
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
        queryBetween, "sports_event_score_update", "current");
  }
}
