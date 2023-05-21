package com.trickl.influxdb.client;

import com.trickl.influxdb.binding.TransactionReader;
import com.trickl.influxdb.binding.TransactionWriter;
import com.trickl.influxdb.persistence.TransactionEntity;
import com.trickl.model.broker.transaction.Transaction;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import com.trickl.model.pricing.statistics.PriceSourceFieldFirstLastDuration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class TransactionClient {

  private final InfluxDbAdapter influxDbClient;

  /**
   * Stores broker transactions in the database.
   *
   * @param temporalPriceSource the temporal price source
   * @param transactions data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(
      TemporalPriceSource temporalPriceSource, List<Transaction> transactions) {
    TransactionWriter transformer = new TransactionWriter(temporalPriceSource);
    List<TransactionEntity> measurements =
        transactions.stream().map(transformer).collect(Collectors.toList());
    return influxDbClient.store(measurements, TransactionEntity.class, TransactionEntity::getTime);
  }

  /**
   * Find broker transactions.
   *
   * @param temporalPriceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of bars
   */
  public Flux<Transaction> findBetween(
      TemporalPriceSource temporalPriceSource, QueryBetween queryBetween) {
    TransactionReader reader = new TransactionReader();
    return influxDbClient
        .findBetween(
            temporalPriceSource.getPriceSource(),
            queryBetween,
            "transaction",
            TransactionEntity.class,
            Collections.singletonMap(
                "simulationId", Collections.singleton(temporalPriceSource.getTemporalSource())))
        .map(reader);
  }

  /**
   * Find a summary of transaction updates between a period of time, grouped by instrument.
   *
   * @param queryBetween A time window there series must have a data point within
   * @param priceSource The price source
   * @return A list of series, including the first and last value of a field
   */
  public Flux<PriceSourceFieldFirstLastDuration> findSummary(
      QueryBetween queryBetween, PriceSource priceSource) {
    return influxDbClient.findFieldFirstLastCountByDay(
        queryBetween, "transaction", "price", priceSource);
  }
}
