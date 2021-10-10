package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticDoubleValueEntity;
import com.trickl.model.analytics.InstantDouble;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AnalyticDoubleValueWriter
    implements Function<InstantDouble, AnalyticDoubleValueEntity> {

  private final TemporalPriceSource temporalPriceSource;

  @Override
  public AnalyticDoubleValueEntity apply(InstantDouble instantDouble) {
    PriceSource priceSource = temporalPriceSource.getPriceSource();
    return AnalyticDoubleValueEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .temporalSource(temporalPriceSource.getTemporalSource())
        .time(instantDouble.getTime())
        .value(instantDouble.getValue())        
        .build();
  }
}
