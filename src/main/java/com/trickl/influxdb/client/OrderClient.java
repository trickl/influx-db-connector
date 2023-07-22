package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.trickl.influxdb.binding.OrderReader;
import com.trickl.influxdb.binding.OrderWriter;
import com.trickl.influxdb.persistence.OrderEntity;
import com.trickl.model.analytics.InstantDouble;
import com.trickl.model.pricing.primitives.Order;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceDouble;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import com.trickl.model.pricing.statistics.PriceSourceInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class OrderClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores quotes in the database.
   *
   * @param priceSource the instrument identifier
   * @param orders data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(PriceSource priceSource, List<Order> orders) {
    OrderWriter transformer = new OrderWriter(priceSource);
    List<OrderEntity> measurements = orders.stream().map(transformer).collect(Collectors.toList());
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    return influxDbStorage.store(measurements, OrderEntity.class, OrderEntity::getTime);
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<Order> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    OrderReader reader = new OrderReader();
    InfluxDbFindBetween finder = new InfluxDbFindBetween(this.influxDbClient, bucket);
    return finder.findBetween(priceSource, queryBetween, "order", OrderEntity.class).map(reader);
  }

  /**
   * Find a summary of price updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration influxDbClient =
        new InfluxDbFirstLastDuration(this.influxDbClient, bucket);
    return influxDbClient.firstLastDuration(queryBetween, "order", "price", priceSource);
  }

  /**
   * Find a count of orders between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Mono<Integer> count(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbCount finder = new InfluxDbCount(influxDbClient, bucket);
    return finder
        .count(queryBetween, "order", "price", priceSource, Optional.of("r.depth == \"0\""))
        .map(PriceSourceInteger::getValue);
  }

  /**
   * Find a count of orders between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Counts by instruments
   */
  public Mono<Double> averageSpread(QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbAverageSpread spreadBetween = new InfluxDbAverageSpread(this.influxDbClient, bucket);
    return spreadBetween
        .averageSpread(
            queryBetween,
            "order",
            "price",
            "order",
            "price",
            priceSource,
            Optional.of("r.depth == \"0\" and r.bidOrAsk == \"B\""),
            Optional.of("r.depth == \"0\" and r.bidOrAsk == \"A\""))
        .map(PriceSourceDouble::getValue);
  }

  /**
   * Get windowed averages over a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return Windowed averages by instruments
   */
  public Flux<InstantDouble> windowedAverages(
      QueryBetween queryBetween, PriceSource priceSource, String windowPeriod) {
    InfluxDbWindowedAverages spreadBetween =
        new InfluxDbWindowedAverages(this.influxDbClient, bucket);
    return spreadBetween
        .windowedAverages(
            queryBetween,
            "order",
            "price",
            "order",
            "price",
            windowPeriod,
            priceSource,
            Optional.of("r.depth == \"0\" and r.bidOrAsk == \"B\""),
            Optional.of("r.depth == \"0\" and r.bidOrAsk == \"A\""))
        .map(
            row -> {
              return new InstantDouble(row.getTime(), row.getValue());
            });
  }
}
