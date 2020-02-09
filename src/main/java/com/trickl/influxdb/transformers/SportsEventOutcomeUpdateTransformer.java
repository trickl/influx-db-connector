package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.SportsEventOutcomeUpdateEntity;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.pricing.primitives.PriceSource;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventOutcomeUpdateTransformer implements
    Function<SportsEventOutcomeUpdate, SportsEventOutcomeUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventOutcomeUpdateEntity apply(SportsEventOutcomeUpdate instrumentEvent) {
    return SportsEventOutcomeUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId())
        .exchangeId(priceSource.getExchangeId())
        .eventId(instrumentEvent.getEventId())
        .time(instrumentEvent.getTime())
        .outcome(instrumentEvent.getOutcome().toString())
        .description(instrumentEvent.getDescription())
        .build();
  }
}