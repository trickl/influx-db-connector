package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventOutcomeUpdateEntity;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventOutcomeUpdateWriter
    implements Function<SportsEventOutcomeUpdate, SportsEventOutcomeUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventOutcomeUpdateEntity apply(SportsEventOutcomeUpdate instrumentEvent) {
    return SportsEventOutcomeUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .outcome(
            instrumentEvent.getOutcome() != null ? instrumentEvent.getOutcome().toString() : null)
        .description(instrumentEvent.getDescription())
        .build();
  }
}
