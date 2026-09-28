package com.trickl.influxdb.binding;

import com.trickl.influxdb.persistence.VolumeUpdateEntity;
import com.trickl.model.event.VolumeUpdate;
import java.util.function.Function;

public class VolumeUpdateReader implements Function<VolumeUpdateEntity, VolumeUpdate> {

  @Override
  public VolumeUpdate apply(VolumeUpdateEntity instrumentEventEntity) {
    return VolumeUpdate.builder()
        .time(instrumentEventEntity.getTime())
        .volume(instrumentEventEntity.getVolume())
        .build();
  }
}
