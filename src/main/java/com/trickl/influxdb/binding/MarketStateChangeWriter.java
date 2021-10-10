package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MarketStateChangeWriter
    implements Function<MarketStateChange, MarketStateChangeEntity> {

  private final PriceSource priceSource;

  @Override
  public MarketStateChangeEntity apply(MarketStateChange instrumentEvent) {
    return MarketStateChangeEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .state(instrumentEvent.getState() != null ? instrumentEvent.getState().toString() : null)
        .description(instrumentEvent.getDescription())
        .build();
  }
}
