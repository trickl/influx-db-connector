package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.TransactionEntity;
import com.trickl.model.broker.transaction.Transaction;
import com.trickl.model.pricing.primitives.TemporalPriceSource;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TransactionWriter implements Function<Transaction, TransactionEntity> {

  private final TemporalPriceSource temporalPriceSource;

  @Override
  public TransactionEntity apply(Transaction transaction) {
    return TransactionEntity.builder()
        .time(transaction.getTime())
        .instrumentId(temporalPriceSource.getPriceSource().getInstrumentId().toUpperCase())
        .exchangeId(temporalPriceSource.getPriceSource().getExchangeId().toUpperCase())
        .simulationId(temporalPriceSource.getTemporalSource())
        .value(
            Optional.ofNullable(transaction.getValue()).map(BigDecimal::doubleValue).orElse(null))
        .accountId(transaction.getAccountId())
        .orderId(transaction.getOrderId())
        .brokerId(transaction.getId())
        .type(Optional.ofNullable(transaction.getType()).map(Object::toString).orElse(null))
        .build();
  }
}
