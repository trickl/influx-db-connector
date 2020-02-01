package com.trickl.influxdb.client;

import com.trickl.model.pricing.primitives.Candle;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.influxdb.dto.Point;

public class CandleClient extends BaseClient<Candle> {

  public CandleClient(InfluxDbClient influxDbClient) {
    super(influxDbClient);
  }

  @Override
  protected String getDatabaseName() {
    return "price";
  }

  @Override
  protected String getMeasurementName() {
    return "ohlvc_bar";
  }

  @Override
  protected List<String> getColumnNames() {
    return Arrays.asList("open", "high", "low", "close");
  }

  @Override
  protected void addFields(Candle bar, Point.Builder builder) {
    builder.addField("open", bar.getOpen());
    builder.addField("high", bar.getHigh());
    builder.addField("low", bar.getLow());
    builder.addField("close", bar.getClose());
  }

  @Override
  protected Candle decodeFromDatabase(
      String instrumentId, Instant time, List<Integer> columnIndexes, List<Object> data) {

    BigDecimal open = BigDecimal.valueOf((Double) data.get(columnIndexes.get(0)));
    BigDecimal high = BigDecimal.valueOf((Double) data.get(columnIndexes.get(1)));
    BigDecimal low = BigDecimal.valueOf((Double) data.get(columnIndexes.get(2)));
    BigDecimal close = BigDecimal.valueOf((Double) data.get(columnIndexes.get(3)));

    return Candle.builder()
        .time(time)
        .open(open)
        .high(high)
        .low(low)        
        .close(close)
        .complete(true)
        .build();
  }

  @Override
  public Function<Candle, Instant> getTimeAccessor() {
    return Candle::getTime;
  }
}
