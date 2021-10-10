package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventIncidentEntity;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SportsEventIncidentWriter
    implements Function<SportsEventIncident, SportsEventIncidentEntity> {

  private final PriceSource priceSource;

  @Override
  public SportsEventIncidentEntity apply(SportsEventIncident instrumentEvent) {
    return SportsEventIncidentEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .matchTime(instrumentEvent.getMatchTime())
        .incidentType(
            instrumentEvent.getIncidentType() != null
                ? instrumentEvent.getIncidentType().toString()
                : null)
        .period(instrumentEvent.getPeriod() != null ? instrumentEvent.getPeriod().toString() : null)
        .side(instrumentEvent.getSide() != null ? instrumentEvent.getSide().toString() : null)
        .build();
  }
}
