package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticStringValueEntity;
import com.trickl.model.analytics.AnalyticId;
import com.trickl.model.analytics.InstantString;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AnalyticStringValueWriter
    implements Function<InstantString, AnalyticStringValueEntity> {

  private final AnalyticId analyticId;
  
  private final TemporalPriceSource temporalPriceSource;

  @Override
  public AnalyticStringValueEntity apply(InstantString instantString) {
    PriceSource priceSource = temporalPriceSource.getPriceSource();
    return AnalyticStringValueEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .temporalSource(temporalPriceSource.getTemporalSource())
        .domain(analyticId.getDomain())
        .analyticName(analyticId.getName())
        .parameters(analyticId.getParameters())
        .time(instantString.getTime())
        .value(instantString.getValue())        
        .build();
  }
}
