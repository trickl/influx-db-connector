package com.trickl.influxdb.client;

import com.influxdb.client.reactive.InfluxDBClientReactive;
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
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class TransactionClient {

  private final InfluxDBClientReactive influxDbClient;

  private final String bucket;

  /**
   * Stores broker transactions in the database.
   *
   * @param temporalPriceSource the temporal price source
   * @param transactions data to store
   * @return counts of records stored
   */
  public Flux<Integer> store(
      TemporalPriceSource temporalPriceSource, List<Transaction> transactions) {
    InfluxDbStorage influxDbStorage = new InfluxDbStorage(influxDbClient, bucket);
    TransactionWriter transformer = new TransactionWriter(temporalPriceSource);
    List<TransactionEntity> measurements =
        transactions.stream().map(transformer).collect(Collectors.toList());
    return influxDbStorage.store(measurements, TransactionEntity.class, TransactionEntity::getTime);
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
    InfluxDbFindBetween influxDbClient = new InfluxDbFindBetween(this.influxDbClient, bucket);
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
  public Mono<PriceSourceFieldFirstLastDuration> firstLastDuration(
      QueryBetween queryBetween, PriceSource priceSource) {
    InfluxDbFirstLastDuration influxDbClient =
        new InfluxDbFirstLastDuration(this.influxDbClient, bucket);
    return influxDbClient.firstLastDuration(
        queryBetween, "transaction", "price", priceSource);
  }
}
