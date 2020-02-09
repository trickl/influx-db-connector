package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.SportsEventOutcomeUpdateEntity;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import com.trickl.model.instrument.EventOutcomeType;

import java.util.function.Function;

public class SportsEventOutcomeUpdateReader 
    implements Function<SportsEventOutcomeUpdateEntity, SportsEventOutcomeUpdate> {

  @Override
  public SportsEventOutcomeUpdate apply(SportsEventOutcomeUpdateEntity instrumentEventEntity) {
    return SportsEventOutcomeUpdate.builder()
        .eventId(instrumentEventEntity.getEventId())
        .time(instrumentEventEntity.getTime())
        .outcome(EventOutcomeType.valueOf(instrumentEventEntity.getOutcome()))
        .description(instrumentEventEntity.getDescription())
        .build();
  }
}