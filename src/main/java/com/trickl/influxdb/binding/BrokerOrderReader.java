package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.BidOrAskFlags;
import com.trickl.influxdb.persistence.BrokerOrderEntity;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.broker.orders.LongShort;
import com.trickl.model.broker.orders.Order;
import com.trickl.model.broker.orders.OrderState;
import com.trickl.model.broker.orders.OrderType;
import com.trickl.model.broker.orders.TimeInForce;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

public class BrokerOrderReader implements Function<BrokerOrderEntity, Order> {

  @Override
  public Order apply(BrokerOrderEntity orderEntity) {
    double price = Optional.ofNullable(orderEntity.getPrice()).orElse(Double.NaN);
    long volume = Optional.ofNullable(orderEntity.getVolume()).orElse(0L);
    double quantityUnfilled = Optional.ofNullable(orderEntity.getQuantityUnfilled()).orElse(0.0);
    double quantityFilled = Optional.ofNullable(orderEntity.getQuantityFilled()).orElse(0.0);
    return Order.builder()
        .price(Double.isNaN(price) ? BigDecimal.ZERO : BigDecimal.valueOf(price))
        .quantity(BigDecimal.valueOf(volume))
        .exchangeId(orderEntity.getExchangeId().toUpperCase())
        .instrumentId(orderEntity.getInstrumentId().toUpperCase())
        .simulationId(orderEntity.getSimulationId())
        .longShort(
            orderEntity.getBidOrAsk().equals(BidOrAskFlags.BID) ? LongShort.Long : LongShort.Short)
        .lastModifiedTime(orderEntity.getTime())
        .createdAtTime(
            Rfc3339.YMDHMSM_FORMATTER.parse(orderEntity.getCreatedAtTime(), Instant::from))
        .quantityUnfilled(BigDecimal.valueOf(quantityUnfilled))
        .quantityFilled(BigDecimal.valueOf(quantityFilled))
        .id(orderEntity.getBrokerId())
        .clientReference(orderEntity.getClientReference())
        .timeInForce(
            Optional.ofNullable(orderEntity.getTimeInForce())
                .map(TimeInForce::valueOf)
                .orElse(null))
        .type(Optional.ofNullable(orderEntity.getType()).map(OrderType::valueOf).orElse(null))
        .reason(orderEntity.getReason())
        .state(OrderState.valueOf(orderEntity.getState()))
        .build();
  }
}
