from(bucket:"prices")
|> range(start: 2021-05-03T16:30:00Z, stop: 2021-05-03T16:40:00Z) 
|> filter(fn: (r) => 
   r._measurement == "order" and
   r._field == "price" and
   r.exchangeId == "SMARKETS" and
   r.instrumentId == "14441062-48923863" and
   r.depth == "0" and
   r.bidOrAsk == "B"
  )
|> drop(columns: ["depth", "bidOrAsk", "_field", "_measurement"])
|> sort(columns: ["_time"], desc: false)
|> window(every: 1m)
|> reduce(fn: (r, accumulator) => ({
    open: if accumulator.count == 0 then r._value else accumulator.open,
    high: if r._value > accumulator.high then r._value else accumulator.high,
    low: if r._value < accumulator.low then r._value else accumulator.low,
    close: r._value,
    count: accumulator.count + 1
  }),
  identity: {open: 0.0, high: 0.0, low: 999999.0, close: 0.0, count: 0}
)
|> drop(columns: ["count"])
|> duplicate(column: "_stop", as: "_time")
|> set(key: "_measurement", value: "best_bid_1m")
|> to(
    bucket: "prices",
    org: "snowfox",
    tagColumns: ["exchangeId", "instrumentId"],
    fieldFn: (r) => ({"open": r.open, "high": r.high, "low": r.low, "close": r.close})
