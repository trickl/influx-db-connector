package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventPeriodUpdateEntity;
import com.trickl.model.event.sports.SportsEventPeriodUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventPeriodUpdateWriter
    implements Function<SportsEventPeriodUpdate, SportsEventPeriodUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventPeriodUpdateEntity apply(SportsEventPeriodUpdate instrumentEvent) {    
    return SportsEventPeriodUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .period(instrumentEvent.getPeriod() != null ? instrumentEvent.getPeriod().toString() : null)
        .build();
  }
}
