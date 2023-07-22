sparkLine = (measurement, field, start, stop) => {
  bids = from(bucket:"prices")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == measurement and r._field == field and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105" and r.depth == "0" and r.bidOrAsk == "B")
  |> group(columns: ["instrumentId", "exchangeId"])
  |> window(every: 5m)
  |> median()

  asks = from(bucket:"prices")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == measurement and r._field == field and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105" and r.depth == "0" and r.bidOrAsk == "A")
  |> group(columns: ["instrumentId", "exchangeId"])
  |> window(every: 5m)
  |> median()

  return join( tables: {f:bids, l:asks}, on: ["exchangeId", "instrumentId", "_stop"])
    |> map(fn: (r) => ({
      _time: r._stop,
      _value: (r._value_l + r._value_f) / 2.0,
      exchangeId: r.exchangeId,
      instrumentId: r.instrumentId
   }))
}

sparkLine(measurement: "order", field: "price", start: 2021-07-16T10:15:00Z, stop: 2024-07-16T10:18:00Z)
