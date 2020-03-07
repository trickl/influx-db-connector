package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.SportsEventIncidentEntity;
import com.trickl.model.event.sports.SportsEventIncident;
import com.trickl.model.event.sports.SportsEventIncidentType;
import com.trickl.model.event.sports.SportsEventPeriod;
import com.trickl.model.event.sports.SportsEventSide;
import java.util.function.Function;

public class SportsEventIncidentReader
    implements Function<SportsEventIncidentEntity, SportsEventIncident> {

  @Override
  public SportsEventIncident apply(SportsEventIncidentEntity instrumentEventEntity) {
    return SportsEventIncident.builder()
        .eventId(instrumentEventEntity.getEventId())
        .time(instrumentEventEntity.getTime())
        .matchTime(instrumentEventEntity.getMatchTime())
        .incidentType(SportsEventIncidentType.valueOf(instrumentEventEntity.getIncidentType()))
        .period(SportsEventPeriod.valueOf(instrumentEventEntity.getPeriod()))
        .side(SportsEventSide.valueOf(instrumentEventEntity.getSide()))
        .build();
  }
}
