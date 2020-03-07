package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.OhlcvBarEntity;
import com.trickl.model.pricing.primitives.Candle;
import java.math.BigDecimal;
import java.util.function.Function;

public class CandleReader implements Function<OhlcvBarEntity, Candle> {

  @Override
  public Candle apply(OhlcvBarEntity barEntity) {
    return Candle.builder()
        .time(barEntity.getTime())
        .open(BigDecimal.valueOf(barEntity.getOpen()))
        .high(BigDecimal.valueOf(barEntity.getHigh()))
        .low(BigDecimal.valueOf(barEntity.getLow()))
        .close(BigDecimal.valueOf(barEntity.getClose()))
        .complete(true)
        .build();
  }
}
