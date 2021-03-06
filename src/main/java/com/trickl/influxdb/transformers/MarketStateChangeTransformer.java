package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MarketStateChangeTransformer
    implements Function<MarketStateChange, MarketStateChangeEntity> {

  private final PriceSource priceSource;

  @Override
  public MarketStateChangeEntity apply(MarketStateChange instrumentEvent) {
    return MarketStateChangeEntity.builder()
        .instrumentId(priceSource.getInstrumentId())
        .exchangeId(priceSource.getExchangeId())
        .time(instrumentEvent.getTime())
        .state(instrumentEvent.getState() != null ? instrumentEvent.getState().toString() : null)
        .description(instrumentEvent.getDescription())
        .build();
  }
}
