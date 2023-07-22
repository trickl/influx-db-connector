package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
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
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class SportsEventMatchTimeUpdateClient {

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
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventMatchTimeUpdate> events) {
    SportsEventMatchTimeUpdateWriter transformer =
        new SportsEventMatchTimeUpdateWriter(priceSource);
    List<SportsEventMatchTimeUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
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
    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
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

    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
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
    String measurementName =
        MessageFormat.format(
            "{0}_{1}",
            eventSource.getEventType(), aggregateEventWidth.toString().substring(2).toLowerCase());
    InfluxDbAggregator influxDbAggregator =
        new InfluxDbAggregator(influxDbClient, bucket, organisation);
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
  public Flux<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration finder =
        new InfluxDbFirstLastDuration(influxDbClient, bucket);
    return finder.firstLastDuration(
        queryBetween, "sports_event_match_time_update", "remaining_time", priceSource);
  }

  /**
   * Find a count of match time updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Flux<PriceSourceInteger> count(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbClient = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbClient.count(
        queryBetween,
        "sports_event_match_time_update",
        "remaining_time",
        priceSource,
        Optional.empty());
  }
}
