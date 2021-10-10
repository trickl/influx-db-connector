package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventMatchTimeUpdateEntity;
import com.trickl.model.event.sports.SportsEventMatchTime;
import com.trickl.model.event.sports.SportsEventMatchTimeUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventMatchTimeUpdateWriter
    implements Function<SportsEventMatchTimeUpdate, SportsEventMatchTimeUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventMatchTimeUpdateEntity apply(SportsEventMatchTimeUpdate instrumentEvent) {
    SportsEventMatchTime matchTime = instrumentEvent.getMatchTime();
    return SportsEventMatchTimeUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .matchTime(matchTime.getMatchTime() != null ? matchTime.getMatchTime().toString() : null)
        .remainingTime(
            matchTime.getRemainingTime() != null ? matchTime.getRemainingTime().toString() : null)
        .remainingTimeInPeriod(
            matchTime.getRemainingTimeInPeriod() != null
                ? matchTime.getRemainingTimeInPeriod().toString()
                : null)
        .build();
  }
}
