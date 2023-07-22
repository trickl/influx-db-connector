count = (measurement, field, start, stop) => {
  return from(bucket:"prices")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == measurement and r._field == field and r.exchangeId == "SMARKETS" and r.instrumentId == "47202945-136383105")
  |> count()
}

count(measurement: "order", field: "price", start: 2021-07-22T16:42:06Z, stop: 2023-07-22T16:42:06Z) 
