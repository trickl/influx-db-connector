package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.AggregatedSportsEventIncidentReader;
import com.trickl.influxdb.binding.SportsEventIncidentReader;
import com.trickl.influxdb.binding.SportsEventIncidentWriter;
import com.trickl.influxdb.persistence.AggregatedSportsEventIncidentEntity;
import com.trickl.influxdb.persistence.SportsEventIncidentEntity;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.event.sports.SportsEventIncidentCategoryType;
import com.trickl.model.event.sports.SportsEventIncidentType;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SportsEventIncidentClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  private final String organisation;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return number of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<SportsEventIncident> events) {
    SportsEventIncidentWriter transformer = new SportsEventIncidentWriter(priceSource);
    List<SportsEventIncidentEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
        measurements, SportsEventIncidentEntity.class, SportsEventIncidentEntity::getTime);
  }

  /**
   * Find all incidents.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<SportsEventIncident> findBetween(EventSource eventSource, QueryBetween queryBetween) {
    SportsEventIncidentReader reader = new SportsEventIncidentReader();

    Set<String> incidentTypes = getIncidentTypes(eventSource);
    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            "sports_event_incident",
            SportsEventIncidentEntity.class,
            incidentTypes != null && !incidentTypes.isEmpty()
                ? Collections.singletonMap("incidentType", incidentTypes)
                : Collections.emptyMap())
        .map(reader);
  }

  /**
   * Find aggregated incidents.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<AggregatedInstrumentEvents> findAggregatedBetween(
      EventSource eventSource, QueryBetween queryBetween) {
    AggregatedSportsEventIncidentReader reader = new AggregatedSportsEventIncidentReader();
    InfluxDbFindBetween finder = new InfluxDbFindBetween(influxDbClient, bucket);
    return finder
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            eventSource.getEventSubType(),
            AggregatedSportsEventIncidentEntity.class)
        .map(reader);
  }

  /**
   * Aggregate incidents.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @param aggregateEventWidth The period of an aggregation window
   * @return A list of bars
   */
  public Flux<AggregatedInstrumentEvents> aggregateBetween(
      EventSource eventSource, QueryBetween queryBetween, Duration aggregateEventWidth) {
    AggregatedSportsEventIncidentReader reader = new AggregatedSportsEventIncidentReader();
    String measurementName =
        MessageFormat.format(
            "{0}_{1}",
            eventSource.getEventSubType(),
            aggregateEventWidth.toString().substring(2).toLowerCase());
    Set<String> incidentTypes = getIncidentTypes(eventSource);
    InfluxDbAggregator influxDbAggregator =
        new InfluxDbAggregator(influxDbClient, bucket, organisation);
    return influxDbAggregator
        .aggregateSportsEventIncidentsBetween(
            eventSource.getPriceSource(),
            queryBetween,
            measurementName,
            aggregateEventWidth,
            incidentTypes != null && !incidentTypes.isEmpty()
                ? Optional.of(Pair.of("incidentType", incidentTypes))
                : Optional.empty())
        .map(reader);
  }

  /**
   * Find a summary of incident updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration finder = new InfluxDbFirstLastDuration(influxDbClient, bucket);
    return finder.firstLastDuration(
        queryBetween, "sports_event_incident", "matchTime", priceSource);
  }

  private Set<String> getIncidentTypes(EventSource eventSource) {
    if (eventSource.getEventSubType() != null) {
      Optional<SportsEventIncidentCategoryType> category =
          SportsEventIncidentCategoryType.tryParse(eventSource.getEventSubType());
      if (category.isPresent()) {
        return category.get().getIncidentTypes().stream()
            .map(type -> type.toString())
            .collect(Collectors.toSet());
      } else {
        Optional<SportsEventIncidentType> incidentType =
            SportsEventIncidentType.tryParse(eventSource.getEventSubType());
        if (incidentType.isPresent()) {
          return Set.of(incidentType.get().toString());
        } else {
          return Collections.emptySet();
        }
      }
    }
    return Collections.emptySet();
  }

  /**
   * Find a count of incidents between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbClient = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbClient
        .count(queryBetween, "sports_event_incident", "matchTime", priceSource)
        .map(PriceSourceInteger::getValue);
  }
}
