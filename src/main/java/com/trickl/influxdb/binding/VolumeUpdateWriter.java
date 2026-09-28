package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.VolumeUpdateEntity;
import com.trickl.model.event.VolumeUpdate;
import com.trickl.model.pricing.primitives.PriceSource;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VolumeUpdateWriter implements Function<VolumeUpdate, VolumeUpdateEntity> {

  private final PriceSource priceSource;

  @Override
  public VolumeUpdateEntity apply(VolumeUpdate instrumentEvent) {
    return VolumeUpdateEntity.builder()
        .instrumentId(priceSource.getInstrumentId().toUpperCase())
        .exchangeId(priceSource.getExchangeId().toUpperCase())
        .time(instrumentEvent.getTime())
        .volume(instrumentEvent.getVolume())
        .build();
  }
}
