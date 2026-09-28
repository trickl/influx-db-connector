package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.TradeUpdateEntity;
import com.trickl.model.event.TradeSide;
import com.trickl.model.event.TradeUpdate;
import java.math.BigDecimal;
import java.util.function.Function;

public class TradeUpdateReader implements Function<TradeUpdateEntity, TradeUpdate> {

  @Override
  public TradeUpdate apply(TradeUpdateEntity instrumentEventEntity) {
    return TradeUpdate.builder()
        .time(instrumentEventEntity.getTime())
        .price(
            instrumentEventEntity.getPrice() != null
                ? BigDecimal.valueOf(instrumentEventEntity.getPrice())
                : null)
        .volume(instrumentEventEntity.getVolume())
        .side(
            instrumentEventEntity.getSide() != null
                ? TradeSide.valueOf(instrumentEventEntity.getSide())
                : null)
        .build();
  }
}
