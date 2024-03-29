package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventOutcomeUpdateEntity;
import com.trickl.model.event.EventOutcomeType;
import com.trickl.model.event.sports.SportsEventOutcomeUpdate;
import java.util.function.Function;

public class SportsEventOutcomeUpdateReader
    implements Function<SportsEventOutcomeUpdateEntity, SportsEventOutcomeUpdate> {

  @Override
  public SportsEventOutcomeUpdate apply(SportsEventOutcomeUpdateEntity instrumentEventEntity) {
    return SportsEventOutcomeUpdate.builder()
        .time(instrumentEventEntity.getTime())
        .outcome(instrumentEventEntity.getOutcome() != null 
            ? EventOutcomeType.valueOf(instrumentEventEntity.getOutcome()) : null)
        .description(instrumentEventEntity.getDescription())
        .build();
  }
}
