package com.trickl.influxdb.client;

import com.trickl.influxdb.binding.AggregatedSportsEventMatchTimeUpdateReader;
import com.trickl.influxdb.binding.SportsEventMatchTimeUpdateReader;
import com.trickl.influxdb.binding.SportsEventMatchTimeUpdateWriter;
import com.trickl.influxdb.persistence.AggregatedSportsEventMatchTimeUpdateEntity;
import com.trickl.influxdb.persistence.SportsEventMatchTimeUpdateEntity;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventMatchTimeUpdate;
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
public class SportsEventMatchTimeUpdateClient {

  private final InfluxDbAdapter influxDbClient;

  private final InfluxDbAggregator influxDbAggregator;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return the number of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventMatchTimeUpdate> events) {
    SportsEventMatchTimeUpdateWriter transformer =
        new SportsEventMatchTimeUpdateWriter(priceSource);
    List<SportsEventMatchTimeUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements,
        SportsEventMatchTimeUpdateEntity.class,
        SportsEventMatchTimeUpdateEntity::getTime);
  }

  /**
   * Find sports updates.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventMatchTimeUpdate> findBetween(
      EventSource eventSource, QueryBetween queryBetween) {
    SportsEventMatchTimeUpdateReader reader = new SportsEventMatchTimeUpdateReader();
    if (eventSource.getEventSubType() != null) {
      // Sub-types not supported
      return Flux.empty();
    }
    return influxDbClient
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            "sports_event_match_time_update",
            SportsEventMatchTimeUpdateEntity.class)
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
    AggregatedSportsEventMatchTimeUpdateReader reader =
        new AggregatedSportsEventMatchTimeUpdateReader();

    return influxDbClient
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            eventSource.getEventType(),
            AggregatedSportsEventMatchTimeUpdateEntity.class)
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
    AggregatedSportsEventMatchTimeUpdateReader reader =
        new AggregatedSportsEventMatchTimeUpdateReader();
    String measurementName = eventSource.getEventType();
    return influxDbAggregator
        .aggregateSportsEventMatchTimeUpdatesBetween(
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
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(
      QueryBetween queryBetween, Optional<PriceSource> priceSource) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, "sports_event_match_time_update", "remaining_time", priceSource);
  }
}
