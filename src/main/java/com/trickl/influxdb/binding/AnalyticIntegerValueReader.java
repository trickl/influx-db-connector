package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.AnalyticIntegerValueEntity;
import com.trickl.model.analytics.InstantInteger;
import java.util.function.Function;

public class AnalyticIntegerValueReader
    implements Function<AnalyticIntegerValueEntity, InstantInteger> {

  @Override
  public InstantInteger apply(AnalyticIntegerValueEntity analyticIntegerValueEntity) {
    return InstantInteger.builder()
        .time(analyticIntegerValueEntity.getTime())
        .value((analyticIntegerValueEntity.getValue().intValue()))
        .build();
  }
}
