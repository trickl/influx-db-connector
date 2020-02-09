package com.trickl.influxdb.client;

import com.trickl.model.pricing.primitives.Order;
import com.trickl.model.pricing.primitives.OrderBook;
import com.trickl.model.pricing.primitives.PriceSource;
import com.trickl.model.pricing.primitives.Quote;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class OrderBookClient {

  private final OrderClient orderClient;

  /**
  * Stores prices in the database.
  *
  * @param priceSource the instrument identifier
  * @param orderBooks data to store
  */
  public Flux<Integer> store(PriceSource priceSource, List<OrderBook> orderBooks) {
    
    Mono<Flux<Integer>> storeBids = 
        Flux.fromIterable(orderBooks).flatMap(
          orderBook -> getOrders(Flux.fromIterable(
            orderBook.getBids()), true, orderBook.getTime()))
        .collectList()   
        .map(list -> orderClient.store(priceSource, list));

    Mono<Flux<Integer>> storeAsks = 
        Flux.fromIterable(orderBooks).flatMap(
          orderBook -> getOrders(Flux.fromIterable(
            orderBook.getAsks()), false, orderBook.getTime()))
        .collectList()
        .map(list -> orderClient.store(priceSource, list));

    return Flux.merge(storeBids, storeAsks).flatMap(rows -> rows);
  }

  protected Flux<Order> getOrders(Flux<Quote> quotes, boolean isBid, Instant time) {
    return quotes.map(quote -> Order.builder()
        .quote(quote)
        .isBid(isBid)
        .time(time)
        .build());
  }

  /**
   * Find candles.
   *
   * @param priceSource the instrument identifier
   * @param queryBetween Query parameters
   * @return A list of order books
   */
  public Flux<OrderBook> findBetween(PriceSource priceSource, QueryBetween queryBetween) {
    return orderClient.findBetween(priceSource, queryBetween)
      .groupBy(Order::getTime)
      .flatMap(groupFlux -> groupFlux
          .collectList()
          .map(orders ->
            OrderBook.builder()
            .bids(getQuotes(orders, true))
            .asks(getQuotes(orders, false))
            .time(groupFlux.key())
            .build()));
  }

  protected List<Quote> getQuotes(List<Order> orders, boolean isBid) {
    return orders.stream()
      .filter(quote -> isBid)
      .map(Order::getQuote)
      .collect(Collectors.toList());
  }
}
