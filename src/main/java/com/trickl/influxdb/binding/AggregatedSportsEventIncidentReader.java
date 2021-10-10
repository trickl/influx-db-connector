package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AggregatedSportsEventIncidentEntity;
import com.trickl.model.event.AggregatedInstrumentEvents;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.event.sports.SportsEventIncidentType;
import com.trickl.model.event.sports.SportsEventPeriod;
import com.trickl.model.event.sports.SportsEventSide;
import java.time.Instant;
import java.util.function.Function;

public class AggregatedSportsEventIncidentReader
    implements Function<AggregatedSportsEventIncidentEntity, AggregatedInstrumentEvents> {

  @Override
  public AggregatedInstrumentEvents apply(
      AggregatedSportsEventIncidentEntity instrumentEventEntity) {
    SportsEventIncident firstEvent =
        SportsEventIncident.builder()
            .time(
                instrumentEventEntity.getFirstTime() != null
                    ? Instant.parse(instrumentEventEntity.getFirstTime())
                    : null)
            .matchTime(instrumentEventEntity.getFirstMatchTime())
            .incidentType(
                instrumentEventEntity.getFirstIncidentType() != null
                    ? SportsEventIncidentType.valueOf(instrumentEventEntity.getFirstIncidentType())
                    : null)
            .side(
                instrumentEventEntity.getFirstSide() != null
                    ? SportsEventSide.valueOf(instrumentEventEntity.getFirstSide())
                    : null)
            .period(
                instrumentEventEntity.getFirstPeriod() != null
                    ? SportsEventPeriod.valueOf(instrumentEventEntity.getFirstPeriod())
                    : null)
            .build();
    SportsEventIncident lastEvent =
        SportsEventIncident.builder()
            .time(
                instrumentEventEntity.getLastTime() != null
                    ? Instant.parse(instrumentEventEntity.getLastTime())
                    : null)
            .matchTime(instrumentEventEntity.getLastMatchTime())
            .incidentType(
                instrumentEventEntity.getLastIncidentType() != null
                    ? SportsEventIncidentType.valueOf(instrumentEventEntity.getLastIncidentType())
                    : null)
            .side(
                instrumentEventEntity.getLastSide() != null
                    ? SportsEventSide.valueOf(instrumentEventEntity.getLastSide())
                    : null)
            .period(
                instrumentEventEntity.getLastPeriod() != null
                    ? SportsEventPeriod.valueOf(instrumentEventEntity.getLastPeriod())
                    : null)
            .build();
    return AggregatedInstrumentEvents.builder()
        .exchangeId(instrumentEventEntity.getExchangeId().toUpperCase())
        .code(instrumentEventEntity.getInstrumentId().toUpperCase())
        .time(instrumentEventEntity.getTime())
        .firstEvent(firstEvent)
        .lastEvent(lastEvent)
        .count(instrumentEventEntity.getCount())
        .build();
  }
}
