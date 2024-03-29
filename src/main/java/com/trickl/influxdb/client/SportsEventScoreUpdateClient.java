package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.AggregatedSportsEventScoreUpdateReader;
import com.trickl.influxdb.binding.SportsEventScoreUpdateReader;
import com.trickl.influxdb.binding.SportsEventScoreUpdateWriter;
import com.trickl.influxdb.persistence.AggregatedSportsEventScoreUpdateEntity;
import com.trickl.influxdb.persistence.SportsEventScoreUpdateEntity;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SportsEventScoreUpdateClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  private final String organisation;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return the number of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventScoreUpdate> events) {
    SportsEventScoreUpdateWriter transformer = new SportsEventScoreUpdateWriter(priceSource);
    List<SportsEventScoreUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
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
    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
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
    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            eventSource.getEventType(),
            AggregatedSportsEventScoreUpdateEntity.class)
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
    String measurementName =
        MessageFormat.format(
            "{0}_{1}",
            eventSource.getEventType(), aggregateEventWidth.toString().substring(2).toLowerCase());
    InfluxDbAggregator influxDbAggregator =
        new InfluxDbAggregator(influxDbClient, bucket, organisation);
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
   * Find a summary of score updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration finder = new InfluxDbFirstLastDuration(influxDbClient, bucket);
    return finder.firstLastDuration(
        queryBetween, "sports_event_score_update", "current", priceSource);
  }

  /**
   * Find a count of score updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbClient = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbClient
        .count(queryBetween, "sports_event_score_update", "current", priceSource)
        .map(PriceSourceInteger::getValue);
  }
}
