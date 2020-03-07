package com.trickl.influxdb.client;

import com.trickl.model.event.InstrumentEvent;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.event.sports.SportsEventScoreUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.List;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class InstrumentEventClient {

  private final MarketStateChangeClient marketStateChangeClient;
  private final SportsEventIncidentClient sportsEventIncidentClient;
  private final SportsEventOutcomeUpdateClient sportsEventOutcomeUpdateClient;
  private final SportsEventScoreUpdateClient sportsEventScoreUpdateClient;

  /**
   * Stores events in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
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

    return Flux.merge(
            storeMarketsChangeEvents,
            storeSportsEventIncidents,
            storeSportsEventOutcomeUpdates,
            storeSportsEventScoreUpdates)
        .flatMap(rows -> rows);
  }

  /**
   * Find events.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<InstrumentEvent> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    return Flux.mergeOrdered(
        InstrumentEventClient::compareEventTimes,
        marketStateChangeClient.findBetween(priceSource, queryBetween),
        sportsEventIncidentClient.findBetween(priceSource, queryBetween),
        sportsEventOutcomeUpdateClient.findBetween(priceSource, queryBetween),
        sportsEventScoreUpdateClient.findBetween(priceSource, queryBetween));
  }

  protected static int compareEventTimes(InstrumentEvent first, InstrumentEvent second) {
    return first.getTime().compareTo(second.getTime());
  }
}
