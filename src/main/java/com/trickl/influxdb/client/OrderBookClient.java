package com.trickl.influxdb.client;

import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.Quote;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import reactor.core.publisher.Mono;

public class OrderBookClient extends BaseClient<OrderBook> {
  private final int quoteDepth;

  public OrderBookClient(Mono<InfluxDB> influxDbConnection, int quoteDepth) {
    super(influxDbConnection);
    this.quoteDepth = quoteDepth;
  }

  @Override
  protected String getDatabaseName() {
    return "price";
  }

  @Override
  protected String getMeasurementName() {
    return "quote";
  }

  @Override
  protected List<String> getColumnNames() {
    return Stream.concat(
            Stream.of("tradeable"),
            IntStream.range(0, quoteDepth)
                .mapToObj(depth -> depth)
                .flatMap(
                    depth ->
                        Arrays.asList("Price" + depth, "Volume" + depth).stream()
                            .flatMap(
                                suffix ->
                                    Arrays.asList("bid" + suffix, "ask" + suffix).stream())))
        .collect(Collectors.toList());
  }

  @Override
  protected void addFields(OrderBook price, Point.Builder builder) {
    IntStream.range(0, quoteDepth)
        .forEach(
            depth -> {
              if (price.getBids().size() > depth) {
                Quote bid = price.getBids().get(depth);
                builder.addField("bidPrice" + depth, bid.getPrice());
                builder.addField("bidVolume" + depth, bid.getVolume());
              }

              if (price.getAsks().size() > depth) {
                Quote ask = price.getAsks().get(depth);
                builder.addField("askPrice" + depth, ask.getPrice());
                builder.addField("askVolume" + depth, ask.getVolume());
              }
            });
  }

  @Override
  protected OrderBook decodeFromDatabase(
      String instrumentId, Instant time, List<Integer> columnIndexes, List<Object> data) {

    OrderBook.OrderBookBuilder builder = OrderBook.builder().time(time);

    IntStream.range(0, quoteDepth)
        .forEach(
            depth -> {
              if (columnIndexes.get(depth) >= 0 && columnIndexes.get(depth + 2) >= 0) {
                Quote bid = getQuote(columnIndexes, data, depth);                
                builder.bid(bid);
              }

              if (columnIndexes.get(depth + 1) >= 0 && columnIndexes.get(depth + 3) >= 0) {
                Quote ask = getQuote(columnIndexes, data, depth + 1);                
                builder.ask(ask);
              }
            });

    return builder.build();
  }

  protected Quote getQuote(List<Integer> columnIndexes, List<Object> data, int depth) {
    BigDecimal price = BigDecimal.valueOf((Double) data.get(columnIndexes.get(depth)));
    Long bidVolume = (Long) data.get(columnIndexes.get(2 + depth));
    return Quote.builder()
        .price(price)
        .volume(Optional.ofNullable(bidVolume).orElse(0L))
        .build();
  }

  @Override
  public Function<OrderBook, Instant> getTimeAccessor() {
    return OrderBook::getTime;
  }
}
