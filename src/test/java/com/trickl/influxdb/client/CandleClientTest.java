package com.trickl.influxdb.client;

import com.trickl.influxdb.config.InfluxDbConfiguration;
import java.io.IOException;
import org.influxdb.InfluxDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"unittest"})
@SpringBootTest(classes = InfluxDbConfiguration.class)
public class CandleClientTest {

  @Mock private InfluxDB influxDb;

  private InfluxDbClient influxDbClient;

  private CandleClient candleClient;

  @AfterEach
  private void shutdown() throws IOException {}

  @Test
  public void testFindByIds() throws IOException {
    // TODO
  }
}
