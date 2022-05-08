package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticBooleanValueEntity;
import com.trickl.model.analytics.InstantBoolean;
import java.util.function.Function;

public class AnalyticBooleanValueReader
    implements Function<AnalyticBooleanValueEntity, InstantBoolean> {

  @Override
  public InstantBoolean apply(AnalyticBooleanValueEntity analyticBooleanValueEntity) {
    return InstantBoolean.builder()
        .time(analyticBooleanValueEntity.getTime())
        .value(analyticBooleanValueEntity.getValue())
        .build();
  }
}
