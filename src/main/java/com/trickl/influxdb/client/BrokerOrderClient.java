package com.trickl.influxdb.client;

import com.trickl.influxdb.binding.BrokerOrderReader;
import com.trickl.influxdb.binding.BrokerOrderWriter;
import com.trickl.influxdb.persistence.BrokerOrderEntity;
import com.trickl.model.broker.orders.Order;
import com.trickl.model.broker.orders.OrderState;
import com.trickl.model.broker.orders.OrderStateFilter;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class BrokerOrderClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores broker orders in the database.
   *
   * @param temporalPriceSource the temporal price source
   * @param orders data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(TemporalPriceSource temporalPriceSource, List<Order> orders) {
    BrokerOrderWriter transformer = new BrokerOrderWriter(temporalPriceSource);
    List<BrokerOrderEntity> measurements =
        orders.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(measurements, BrokerOrderEntity.class, BrokerOrderEntity::getTime);
  }

  /**
   * Find broker orders.
   *
   * @param temporalPriceSource the instrument identifier
   * @param queryBetween Query parameters
   * @param orderStateFilter Types of orders
   * @return A list of bars
   */
  public Flux<Order> findBetween(
      TemporalPriceSource temporalPriceSource,
      QueryBetween queryBetween,
      OrderStateFilter orderStateFilter) {    
    Map<String, Set<String>> filter = new HashMap<>();
    filter.put("simulationId", Collections.singleton(temporalPriceSource.getTemporalSource()));
    Set<String> allowedStates = new HashSet<>();
    switch (orderStateFilter) {
      case CANCELLED:
        allowedStates.add(OrderState.CANCELLED.toString());
        break;
      case FILLED:
        allowedStates.add(OrderState.PARTIAL.toString());
        allowedStates.add(OrderState.FILLED.toString());
        break;
      case PENDING:
        allowedStates.add(OrderState.OPEN.toString());
        break;
      case TRIGGERED:
        allowedStates.add(OrderState.TRIGGERED.toString());
        break;
      case ALL:
      default:
        allowedStates.addAll(
            Stream.of(OrderState.values()).map(Object::toString).collect(Collectors.toList()));
        break;
    }
    filter.put("state", allowedStates);

    BrokerOrderReader reader = new BrokerOrderReader();
    return influxDbClient
        .findBetween(
            temporalPriceSource.getPriceSource(),
            queryBetween,
            "broker_order",
            BrokerOrderEntity.class,
            Collections.singletonMap(
                "simulationId", Collections.singleton(temporalPriceSource.getTemporalSource())))
        .map(reader);
  }

  /**
   * Find a summary of order updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(
      QueryBetween queryBetween, PriceSource priceSource) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, "broker_order", "price", priceSource);
  }
}
