package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.TransactionEntity;
import com.trickl.model.broker.transaction.Transaction;
import com.trickl.model.broker.transaction.TransactionType;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;

public class TransactionReader implements Function<TransactionEntity, Transaction> {

  @Override
  public Transaction apply(TransactionEntity transactionEntity) {
    double value = Optional.ofNullable(transactionEntity.getValue()).orElse(Double.NaN);
    return Transaction.builder()
        .value(Double.isNaN(value) ? BigDecimal.ZERO : BigDecimal.valueOf(value))
        .exchangeId(transactionEntity.getExchangeId().toUpperCase())
        .instrumentId(transactionEntity.getInstrumentId().toUpperCase())
        .simulationId(transactionEntity.getSimulationId())
        .accountId(transactionEntity.getAccountId())
        .orderId(transactionEntity.getOrderId())
        .time(transactionEntity.getTime())
        .id(transactionEntity.getBrokerId())
        .type(
            Optional.ofNullable(transactionEntity.getType())
                .map(TransactionType::valueOf)
                .orElse(null))
        .build();
  }
}
