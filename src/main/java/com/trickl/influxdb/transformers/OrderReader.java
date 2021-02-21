package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.BidOrAskFlags;
import com.trickl.influxdb.persistence.OrderEntity;
import com.trickl.model.pricing.primitives.Order;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;

public class OrderReader implements Function<OrderEntity, Order> {

  @Override
  public Order apply(OrderEntity orderEntity) {
    double price = Optional.ofNullable(orderEntity.getPrice()).orElse(Double.NaN);
    long volume = Optional.ofNullable(orderEntity.getVolume()).orElse(0L);
    return Order.builder()
        .quote(
            Quote.builder()
                .price(Double.isNaN(price) ? BigDecimal.ZERO : BigDecimal.valueOf(price))
                .volume(volume)
                .source(
                    PriceSource.builder()
                        .exchangeId(orderEntity.getExchangeId())
                        .instrumentId(orderEntity.getInstrumentId())
                        .build())
                .build())
        .isBid(orderEntity.getBidOrAsk().equals(BidOrAskFlags.BID))
        .depth(Integer.parseInt(orderEntity.getDepth()))
        .time(orderEntity.getTime())
        .build();
  }
}
