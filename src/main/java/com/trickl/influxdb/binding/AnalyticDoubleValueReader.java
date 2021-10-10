package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticDoubleValueEntity;
import com.trickl.model.analytics.InstantDouble;
import java.util.function.Function;

public class AnalyticDoubleValueReader
    implements Function<AnalyticDoubleValueEntity, InstantDouble> {

  @Override
  public InstantDouble apply(AnalyticDoubleValueEntity analyticDoubleValueEntity) {
    return InstantDouble.builder()
        .time(analyticDoubleValueEntity.getTime())
        .value(analyticDoubleValueEntity.getValue())
        .build();
  }
}
