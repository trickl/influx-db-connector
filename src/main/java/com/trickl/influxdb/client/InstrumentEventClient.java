package com.trickl.influxdb.client;

import com.trickl.model.event.InstrumentEvent;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.event.sports.SportsEventMatchTimeUpdate;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.event.sports.SportsEventPeriodUpdate;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class InstrumentEventClient {

  private final MarketStateChangeClient marketStateChangeClient;
  private final SportsEventIncidentClient sportsEventIncidentClient;
  private final SportsEventOutcomeUpdateClient sportsEventOutcomeUpdateClient;
  private final SportsEventScoreUpdateClient sportsEventScoreUpdateClient;
  private final SportsEventPeriodUpdateClient sportsEventPeriodUpdateClient;
  private final SportsEventMatchTimeUpdateClient sportsEventMatchTimeUpdateClient;

  /**
   * Stores events in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<InstrumentEvent> events) {
    Mono<Flux<Integer>> storeMarketsChangeEvents =
        Flux.fromIterable(events)
            .filter(event -> event instanceof MarketStateChange)
            .cast(MarketStateChange.class)
            .collectList()
            .map(list -> marketStateChangeClient.store(priceSource, list));

    Mono<Flux<Integer>> storeSportsEventIncidents =
        Flux.fromIterable(events)
            .filter(event -> event instanceof SportsEventIncident)
            .cast(SportsEventIncident.class)
            .collectList()
            .map(list -> sportsEventIncidentClient.store(priceSource, list));

    Mono<Flux<Integer>> storeSportsEventOutcomeUpdates =
        Flux.fromIterable(events)
            .filter(event -> event instanceof SportsEventOutcomeUpdate)
            .cast(SportsEventOutcomeUpdate.class)
            .collectList()
            .map(list -> sportsEventOutcomeUpdateClient.store(priceSource, list));

    Mono<Flux<Integer>> storeSportsEventScoreUpdates =
        Flux.fromIterable(events)
            .filter(event -> event instanceof SportsEventScoreUpdate)
            .cast(SportsEventScoreUpdate.class)
            .collectList()
            .map(list -> sportsEventScoreUpdateClient.store(priceSource, list));

    Mono<Flux<Integer>> storeSportsEventPeriodUpdates =
        Flux.fromIterable(events)
            .filter(event -> event instanceof SportsEventPeriodUpdate)
            .cast(SportsEventPeriodUpdate.class)
            .collectList()
            .map(list -> sportsEventPeriodUpdateClient.store(priceSource, list));

    Mono<Flux<Integer>> storeSportsEventMatchTimeUpdates =
        Flux.fromIterable(events)
            .filter(event -> event instanceof SportsEventMatchTimeUpdate)
            .cast(SportsEventMatchTimeUpdate.class)
            .collectList()
            .map(list -> sportsEventMatchTimeUpdateClient.store(priceSource, list));

    return Flux.merge(
            storeMarketsChangeEvents,
            storeSportsEventIncidents,
            storeSportsEventOutcomeUpdates,
            storeSportsEventScoreUpdates,
            storeSportsEventPeriodUpdates,
            storeSportsEventMatchTimeUpdates)
        .flatMap(rows -> rows);
  }

  /**
   * Find events.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstrumentEvent> findBetween(EventSource eventSource, QueryBetween queryBetween) {
    if (eventSource.getEventType() == null) {
      return Flux.merge(
              marketStateChangeClient.findBetween(eventSource, queryBetween),
              sportsEventIncidentClient.findBetween(eventSource, queryBetween),
              sportsEventOutcomeUpdateClient.findBetween(eventSource, queryBetween),
              sportsEventScoreUpdateClient.findBetween(eventSource, queryBetween),
              sportsEventPeriodUpdateClient.findBetween(eventSource, queryBetween),
              sportsEventMatchTimeUpdateClient.findBetween(eventSource, queryBetween))
          .sort(InstrumentEventClient::compareEventTimes);
    }

    boolean isAggregate =
        isAggregateName(
            Optional.ofNullable(eventSource.getEventSubType()).orElse(eventSource.getEventType()));

    String eventTypeLowerCase = eventSource.getEventType().toLowerCase();
    String[] eventTypeParts = eventTypeLowerCase.split("_");
    String eventTypeBase = eventTypeParts[0];

    if (!isAggregate) {
      switch (eventTypeBase) {
        case "market":
          return marketStateChangeClient
              .findBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "incident":
          return sportsEventIncidentClient
              .findBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "outcome":
          return sportsEventOutcomeUpdateClient
              .findBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "score":
          return sportsEventScoreUpdateClient
              .findBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "period":
          return sportsEventPeriodUpdateClient
              .findBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "match_time":
          return sportsEventMatchTimeUpdateClient
              .findBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        default:
          return Flux.error(
              new NoSuchMeasurementTypeException(
                  "EventType: " + eventSource.getEventType() + " not supported."));
      }
    } else {
      switch (eventTypeBase) {
        case "incident":
          return sportsEventIncidentClient
              .findAggregatedBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "score":
          return sportsEventScoreUpdateClient
              .findAggregatedBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "match_time":
          return sportsEventMatchTimeUpdateClient
              .findAggregatedBetween(eventSource, queryBetween)
              .cast(InstrumentEvent.class);
        case "outcome":
          return Flux.error(
              new NoSuchMeasurementTypeException("Aggregate outcome events not supported."));
        case "period":
          return Flux.error(
              new NoSuchMeasurementTypeException("Aggregate period events not supported."));
        case "market":
          return Flux.error(
              new NoSuchMeasurementTypeException("Aggregate market events not supported."));
        default:
          return Flux.error(
              new NoSuchMeasurementTypeException(
                  "EventType: " + eventSource.getEventType() + " not supported."));
      }
    }
  }

  /**
   * Find candles.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @param aggregateEventWidth the period of an aggregation window
   * @return A list of bars
   */
  public Flux<InstrumentEvent> aggregateBetween(
      EventSource eventSource, QueryBetween queryBetween, Duration aggregateEventWidth) {
    if (eventSource.getEventType() == null) {
      return Flux.error(
          new NoSuchMeasurementTypeException("Can only aggregate specific event types."));
    }

    String eventTypeLowerCase = eventSource.getEventType().toLowerCase();
    String[] eventTypeParts = eventTypeLowerCase.split("_");
    String eventTypeBase = eventTypeParts[0];

    switch (eventTypeBase) {
      case "market":
        return Flux.empty();
      case "incident":
        return sportsEventIncidentClient
            .aggregateBetween(eventSource, queryBetween, aggregateEventWidth)
            .cast(InstrumentEvent.class);
      case "outcome":
        return Flux.empty();
      case "period":
        return Flux.empty();
      case "score":
        return sportsEventScoreUpdateClient
            .aggregateBetween(eventSource, queryBetween, aggregateEventWidth)
            .cast(InstrumentEvent.class);
      case "match_time":
        return sportsEventMatchTimeUpdateClient
            .aggregateBetween(eventSource, queryBetween, aggregateEventWidth)
            .cast(InstrumentEvent.class);
      default:
        return Flux.error(
            new NoSuchMeasurementTypeException(
                "EventType: " + eventSource.getEventType() + " not supported."));
    }
  }

  protected static int compareEventTimes(InstrumentEvent first, InstrumentEvent second) {
    return first.getTime().compareTo(second.getTime());
  }

  private boolean isAggregateName(String name) {
    String[] nameParts = name.split("_");
    String lastPart = nameParts[nameParts.length - 1];
    Duration duration = InfluxDbDurationParser.tryParse(lastPart);
    return duration != null;
  }
}
