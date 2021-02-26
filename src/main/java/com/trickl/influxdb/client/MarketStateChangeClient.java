package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.influxdb.transformers.MarketStateChangeReader;
import com.trickl.influxdb.transformers.MarketStateChangeTransformer;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class MarketStateChangeClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param candles data to store
   */
  public Flux<Integer> store(PriceSource priceSource, List<MarketStateChange> candles) {
    MarketStateChangeTransformer transformer = new MarketStateChangeTransformer(priceSource);
    List<MarketStateChangeEntity> measurements =
        candles.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements,
        CommonDatabases.PRICES.getName(),
        MarketStateChangeEntity.class,
        MarketStateChangeEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<MarketStateChange> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    MarketStateChangeReader reader = new MarketStateChangeReader();
    return influxDbClient
        .findBetween(
            priceSource,
            queryBetween,
            CommonDatabases.PRICES.getName(),
            "market_state_change",
            MarketStateChangeEntity.class)
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
        queryBetween, CommonDatabases.PRICES.getName(), "market_state_change", "state");
  }
}
