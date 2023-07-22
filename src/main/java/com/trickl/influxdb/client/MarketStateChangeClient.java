package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.MarketStateChangeReader;
import com.trickl.influxdb.binding.MarketStateChangeWriter;
import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class MarketStateChangeClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<MarketStateChange> events) {
    MarketStateChangeWriter transformer = new MarketStateChangeWriter(priceSource);
    List<MarketStateChangeEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
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
    InfluxDbFindBetween findBetween = new InfluxDbFindBetween(influxDbClient, bucket);
    return findBetween
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            "market_state_change",
            MarketStateChangeEntity.class,
            eventSource.getEventSubType() != null
                ? Collections.singletonMap("state", Set.of(eventSource.getEventSubType()))
                : Collections.emptyMap())
        .map(reader);
  }

  /**
   * Find a summary of changes between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration finder = new InfluxDbFirstLastDuration(this.influxDbClient, bucket);
    return finder.firstLastDuration(queryBetween, "market_state_change", "state", priceSource);
  }

  /**
   * Find a count of changes between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Flux<PriceSourceInteger> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbClient = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbClient.count(queryBetween, "market_state_change", "state", priceSource);
  }
}
