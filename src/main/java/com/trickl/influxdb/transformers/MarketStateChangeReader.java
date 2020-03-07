package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.instrument.MarketState;
import java.util.function.Function;

public class MarketStateChangeReader
    implements Function<MarketStateChangeEntity, MarketStateChange> {

  @Override
  public MarketStateChange apply(MarketStateChangeEntity instrumentEventEntity) {
    return MarketStateChange.builder()
        .eventId(instrumentEventEntity.getEventId())
        .time(instrumentEventEntity.getTime())
        .state(MarketState.valueOf(instrumentEventEntity.getState()))
        .description(instrumentEventEntity.getDescription())
        .build();
  }
}
