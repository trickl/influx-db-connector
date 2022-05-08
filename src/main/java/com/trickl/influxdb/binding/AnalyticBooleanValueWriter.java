package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticBooleanValueEntity;
import com.trickl.model.analytics.AnalyticId;
import com.trickl.model.analytics.InstantBoolean;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AnalyticBooleanValueWriter
    implements Function<InstantBoolean, AnalyticBooleanValueEntity> {

  private final AnalyticId analyticId;
  
  private final TemporalPriceSource temporalPriceSource;

  @Override
  public AnalyticBooleanValueEntity apply(InstantBoolean instantBoolean) {
    PriceSource priceSource = temporalPriceSource.getPriceSource();
    return AnalyticBooleanValueEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .temporalSource(temporalPriceSource.getTemporalSource())
        .domain(analyticId.getDomain())
        .analyticName(analyticId.getName())
        .parameters(analyticId.getParameters())
        .time(instantBoolean.getTime())
        .value(instantBoolean.getValue())        
        .build();
  }
}
