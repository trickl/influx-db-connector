package com.trickl.influxdb.persistence;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Measurement(name = "transaction")
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TransactionEntity {
  @NotNull
  @Column(name = "time", timestamp = true)
  private Instant time;

  @NotNull
  @Column(name = "instrumentId", tag = true)
  private String instrumentId;

  @NotNull
  @Column(name = "exchangeId", tag = true)
  private String exchangeId;

  @Column(name = "simulationId", tag = true)
  private String simulationId;

  @Column(name = "accountId", tag = true)
  private String accountId;

  @Column(name = "orderId", tag = true)
  private String orderId;

  @NotNull
  @Column(name = "brokerId")
  private String brokerId;

  @NotNull
  @Column(name = "type")
  private String type;

  @NotNull
  @Min(0)
  @Column(name = "value")
  protected Double value;
}
