package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.MarketStateChangeEntity;
import com.trickl.model.event.MarketStateChange;
import com.trickl.model.instrument.MarketState;
import java.util.function.Function;

public class MarketStateChangeReader
    implements Function<MarketStateChangeEntity, MarketStateChange> {

  @Override
  public MarketStateChange apply(MarketStateChangeEntity instrumentEventEntity) {
    return MarketStateChange.builder()
        .time(instrumentEventEntity.getTime())
        .state(instrumentEventEntity.getState() != null 
            ? MarketState.valueOf(instrumentEventEntity.getState()) : null)
        .description(instrumentEventEntity.getDescription())
        .build();
  }
}
