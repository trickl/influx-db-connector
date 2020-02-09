package com.trickl.influxdb.transformers;

import com.trickl.influxdb.persistence.OhlcvBarEntity;
import com.trickl.model.pricing.primitives.Candle;
import com.trickl.model.pricing.primitives.PriceSource;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CandleTransformer implements Function<Candle, OhlcvBarEntity> {

  private final PriceSource priceSource;

  @Override
  public OhlcvBarEntity apply(Candle candle) {
    return OhlcvBarEntity.builder()
        .instrumentId(priceSource.getInstrumentId())
        .exchangeId(priceSource.getExchangeId())
        .time(candle.getTime())
        .open(Optional.ofNullable(candle.getOpen()).map(BigDecimal::doubleValue).orElse(null))
        .high(Optional.ofNullable(candle.getHigh()).map(BigDecimal::doubleValue).orElse(null))
        .low(Optional.ofNullable(candle.getLow()).map(BigDecimal::doubleValue).orElse(null))
        .close(Optional.ofNullable(candle.getClose()).map(BigDecimal::doubleValue).orElse(null))
        .build();
  }
}