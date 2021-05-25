package com.trickl.influxdb.client;

import java.text.MessageFormat;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class FluxStatementFilterBuilder {
  /**
   * Build a flux statement representing a filter.
   *
   * @param filter The filter
   * @return A filter filter condition
   */
  public static String buildFrom(Pair<String, Set<String>> filter) {
    String filterField = filter.getKey();
    String filterCondition =
        filter.getValue().stream()
            .map(filterValue -> MessageFormat.format("r.{0} == \"{1}\"", filterField, filterValue))
            .collect(Collectors.joining(" or "));
    return MessageFormat.format("|> filter(fn: (r) => {0})\n", filterCondition);
  }
}
