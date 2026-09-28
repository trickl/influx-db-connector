package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.TradeUpdateEntity;
import com.trickl.model.event.TradeUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TradeUpdateWriter implements Function<TradeUpdate, TradeUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public TradeUpdateEntity apply(TradeUpdate instrumentEvent) {
    return TradeUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .price(instrumentEvent.getPrice() != null ? instrumentEvent.getPrice().doubleValue() : null)
        .volume(instrumentEvent.getVolume())
        .side(instrumentEvent.getSide() != null ? instrumentEvent.getSide().toString() : null)
        .build();
  }
}
