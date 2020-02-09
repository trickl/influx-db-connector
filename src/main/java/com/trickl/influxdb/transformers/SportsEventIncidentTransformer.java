package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.SportsEventIncidentEntity;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.pricing.primitives.PriceSource;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventIncidentTransformer implements
    Function<SportsEventIncident, SportsEventIncidentEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventIncidentEntity apply(SportsEventIncident instrumentEvent) {
    return SportsEventIncidentEntity.builder()
        .instrumentId(priceSource.getInstrumentId())
        .exchangeId(priceSource.getExchangeId())
        .eventId(instrumentEvent.getEventId())
        .time(instrumentEvent.getTime())
        .matchTime(instrumentEvent.getMatchTime())
        .incidentType(instrumentEvent.getIncidentType().toString())
        .period(instrumentEvent.getPeriod().toString())
        .side(instrumentEvent.getSide().toString())
        .build();
  }
}