package com.trickl.influxdb.client;

import com.trickl.influxdb.persistence.OrderEntity;
import com.trickl.influxdb.transformers.OrderReader;
import com.trickl.influxdb.transformers.OrderTransformer;
import com.trickl.model.pricing.primitives.Order;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class OrderClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores prices in the database.
   *
   * @param priceSource the instrument identifier
   * @param orders data to store
   */
  public Flux<Integer> store(PriceSource priceSource, List<Order> orders) {
    OrderTransformer transformer = new OrderTransformer(priceSource);
    List<OrderEntity> measurements = orders.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(
        measurements, CommonDatabases.PRICES.getName(), OrderEntity.class, OrderEntity::getTime);
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
    return influxDbClient
        .findBetween(
            priceSource, queryBetween, CommonDatabases.PRICES.getName(), "order", OrderEntity.class)
        .map(reader);
  }

  /**
   * Find a summary of price updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(QueryBetween queryBetween) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, CommonDatabases.PRICES.getName(), "order", "price");
  }
}
