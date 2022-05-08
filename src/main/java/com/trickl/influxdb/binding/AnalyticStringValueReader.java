package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticStringValueEntity;
import com.trickl.model.analytics.InstantString;
import java.util.function.Function;

public class AnalyticStringValueReader
    implements Function<AnalyticStringValueEntity, InstantString> {

  @Override
  public InstantString apply(AnalyticStringValueEntity analyticStringValueEntity) {
    return InstantString.builder()
        .time(analyticStringValueEntity.getTime())
        .value(analyticStringValueEntity.getValue())
        .build();
  }
}
