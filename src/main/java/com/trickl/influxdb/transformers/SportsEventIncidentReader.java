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
        .time(instrumentEventEntity.getTime())
        .matchTime(instrumentEventEntity.getMatchTime())
        .incidentType(instrumentEventEntity.getIncidentType() != null 
            ? SportsEventIncidentType.valueOf(instrumentEventEntity.getIncidentType()) : null)
        .period(instrumentEventEntity.getPeriod() != null
            ? SportsEventPeriod.valueOf(instrumentEventEntity.getPeriod()) : null)
        .side(instrumentEventEntity.getSide() != null 
            ? SportsEventSide.valueOf(instrumentEventEntity.getSide()) : null)
        .build();
  }
}
