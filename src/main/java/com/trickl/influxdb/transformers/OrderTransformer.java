package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.BidOrAskFlags;
import com.trickl.influxdb.persistence.OrderEntity;
import com.trickl.model.pricing.primitives.Order;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OrderTransformer implements Function<Order, OrderEntity> {

  private final PriceSource priceSource;

  @Override
  public OrderEntity apply(Order order) {
    Quote quote = order.getQuote();
    return OrderEntity.builder()
        .instrumentId(priceSource.getInstrumentId())
        .exchangeId(priceSource.getExchangeId())
        .price(Optional.ofNullable(quote.getPrice()).map(BigDecimal::doubleValue).orElse(null))
        .volume(quote.getVolume())
        .bidOrAsk(order.isBid() ? BidOrAskFlags.BID : BidOrAskFlags.ASK)
        .depth(order.getDepth())
        .time(order.getTime())
        .build();
  }
}
