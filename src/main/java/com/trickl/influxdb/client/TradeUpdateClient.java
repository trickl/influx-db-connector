package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.TradeUpdateReader;
import com.trickl.influxdb.binding.TradeUpdateWriter;
import com.trickl.influxdb.persistence.TradeUpdateEntity;
import com.trickl.model.event.TradeUpdate;
import com.trickl.model.pricing.primitives.EventSource;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class TradeUpdateClient {

  private static final String MEASUREMENT = "trade";

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores trades in the database.
   *
   * @param priceSource the instrument identifier
   * @param events data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<TradeUpdate> events) {
    TradeUpdateWriter transformer = new TradeUpdateWriter(priceSource);
    List<TradeUpdateEntity> measurements =
        events.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(
        measurements, TradeUpdateEntity.class, TradeUpdateEntity::getTime);
  }

  /**
   * Find trades.
   *
   * @param eventSource the instrument identifier
   * @param queryBetween Query parameters
   * @return The trades in time order
   */
  public Flux<TradeUpdate> findBetween(EventSource eventSource, QueryBetween queryBetween) {
    TradeUpdateReader reader = new TradeUpdateReader();
    InfluxDbFindBetween findBetween = new InfluxDbFindBetween(influxDbClient, bucket);
    return findBetween
        .findBetween(
            eventSource.getPriceSource(),
            queryBetween,
            MEASUREMENT,
            TradeUpdateEntity.class,
            Collections.emptyMap())
        .map(reader);
  }

  /**
   * Find a summary of trades between a period of time.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return The first and last value, and the duration between them
   */
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration finder = new InfluxDbFirstLastDuration(this.influxDbClient, bucket);
    return finder.firstLastDuration(queryBetween, MEASUREMENT, "price", priceSource);
  }

  /**
   * Find a count of trades between a period of time.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return The count
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount influxDbCount = new InfluxDbCount(this.influxDbClient, bucket);
    return influxDbCount
        .count(queryBetween, MEASUREMENT, "price", priceSource)
        .map(PriceSourceInteger::getValue);
  }
}
