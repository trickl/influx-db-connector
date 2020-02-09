package com.trickl.influxdb.client;

import com.trickl.model.pricing.primitives.PriceSource;

import java.time.Instant;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PriceSeries {
  protected PriceSource priceSource;
  protected Instant start;
  protected Instant end;
}