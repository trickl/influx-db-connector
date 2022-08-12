package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.BidOrAskFlags;
import com.trickl.influxdb.persistence.BrokerOrderEntity;
import com.trickl.influxdb.text.Rfc3339;
import com.trickl.model.broker.orders.LongShort;
import com.trickl.model.broker.orders.Order;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BrokerOrderWriter implements Function<Order, BrokerOrderEntity> {

  private final TemporalPriceSource temporalPriceSource;

  @Override
  public BrokerOrderEntity apply(Order order) {
    return BrokerOrderEntity.builder()
        .time(order.getLastModifiedTime())
        .instrumentId(temporalPriceSource.getPriceSource().getInstrumentId().toUpperCase())
        .exchangeId(temporalPriceSource.getPriceSource().getExchangeId().toUpperCase())
        .simulationId(temporalPriceSource.getTemporalSource())
        .price(Optional.ofNullable(order.getPrice()).map(BigDecimal::doubleValue).orElse(null))
        .volume(order.getQuantity().longValue())
        .bidOrAsk(order.getLongShort() == LongShort.Long ? BidOrAskFlags.BID : BidOrAskFlags.ASK)
        .createdAtTime(Rfc3339.YMDHMSM_FORMATTER.format(order.getCreatedAtTime()))
        .quantityUnfilled(
            Optional.ofNullable(order.getQuantityUnfilled())
                .map(BigDecimal::longValue)
                .orElse(null))
        .quantityFilled(
            Optional.ofNullable(order.getQuantityFilled()).map(BigDecimal::longValue).orElse(null))
        .brokerId(order.getId())
        .clientReference(order.getClientReference())
        .timeInForce(Optional.ofNullable(order.getTimeInForce()).map(Object::toString).orElse(null))
        .type(Optional.ofNullable(order.getType()).map(Object::toString).orElse(null))
        .reason(order.getReason())
        .state(order.getState().toString())
        .build();
  }
}
