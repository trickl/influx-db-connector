package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.influxdb.transformers.MarketStateChangeReader;
import com.trickl.influxdb.transformers.MarketStateChangeTransformer;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class MarketStateChangeClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<MarketStateChange> events) {
    MarketStateChangeTransformer transformer = new MarketStateChangeTransformer(priceSource);
    List<MarketStateChangeEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements, MarketStateChangeEntity.class, MarketStateChangeEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<MarketStateChange> findBetween(EventSource eventSource, QueryBetween queryBetween) {
    MarketStateChangeReader reader = new MarketStateChangeReader();
    return influxDbClient
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            "market_state_change",
            MarketStateChangeEntity.class,
            eventSource.getEventSubType() != null
                ? Optional.of(Pair.of("state", Set.of(eventSource.getEventSubType())))
                : Optional.empty())
        .map(reader);
  }

  /**
   * Find a summary of changes between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(QueryBetween queryBetween) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, "market_state_change", "state");
  }
}
