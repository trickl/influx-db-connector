package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.SportsEventPeriodUpdateEntity;
import com.trickl.model.event.sports.SportsEventPeriod;
import com.trickl.model.event.sports.SportsEventPeriodUpdate;
import java.util.function.Function;

public class SportsEventPeriodUpdateReader
    implements Function<SportsEventPeriodUpdateEntity, SportsEventPeriodUpdate> {

  @Override
  public SportsEventPeriodUpdate apply(SportsEventPeriodUpdateEntity instrumentEventEntity) {
    return SportsEventPeriodUpdate.builder()
        .time(instrumentEventEntity.getTime())
        .period(
            instrumentEventEntity.getPeriod() != null
                ? SportsEventPeriod.valueOf(instrumentEventEntity.getPeriod())
                : null)
        .build();
  }
}
