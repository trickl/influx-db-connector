package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticIntegerValueEntity;
import com.trickl.model.analytics.AnalyticId;
import com.trickl.model.analytics.InstantInteger;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AnalyticIntegerValueWriter
    implements Function<InstantInteger, AnalyticIntegerValueEntity> {

  private final AnalyticId analyticId;
  
  private final TemporalPriceSource temporalPriceSource;

  @Override
  public AnalyticIntegerValueEntity apply(InstantInteger instantInteger) {
    PriceSource priceSource = temporalPriceSource.getPriceSource();
    return AnalyticIntegerValueEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .temporalSource(temporalPriceSource.getTemporalSource())
        .domain(analyticId.getDomain())
        .analyticName(analyticId.getName())
        .parameters(analyticId.getParameters())
        .time(instantInteger.getTime())
        .value(instantInteger.getValue())        
        .build();
  }
}
