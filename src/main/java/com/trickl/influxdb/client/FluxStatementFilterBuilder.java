package com.trickl.influxdb.client;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

public class FluxStatementFilterBuilder {
  /**
   * Build a flux statement representing a filter.
   *
   * @param filter The filter
   * @return A filter filter condition
   */
  public static String buildFrom(Pair<String, Set<String>> filter) {
    String filterCondition = buildClauseFrom(filter);
    return buildFilterFromClauses(Stream.of(filterCondition));
  }

  /**
   * Build a flux statement representing a filter.
   *
   * @param filter The filter
   * @return A filter filter condition
   */
  public static String buildFrom(Map<String, Set<String>> filter) {
    if (filter.isEmpty()) {
      return "";
    }
    Stream<String> filterConditions =
        filter.entrySet().stream().map(FluxStatementFilterBuilder::buildClauseFrom);
    return buildFilterFromClauses(filterConditions);
  }

  /**
   * Build a OR clause statement.
   *
   * @param filter The filter
   * @return A filter filter condition
   */
  private static String buildClauseFrom(Entry<String, Set<String>> filter) {
    String filterField = filter.getKey();
    return filter.getValue().stream()
        .map(filterValue -> MessageFormat.format("r.{0} == \"{1}\"", filterField, filterValue))
        .collect(Collectors.joining(" or "));
  }

  /**
   * Build a OR clause statement.
   *
   * @param filter The filter
   * @return A filter filter condition
   */
  private static String buildFilterFromClauses(Stream<String> clauses) {
    return MessageFormat.format(
        "|> filter(fn: (r) => {0})\n",
        clauses
            .map(clause -> MessageFormat.format("({0})", clause))
            .collect(Collectors.joining(" and ")));
  }
}
