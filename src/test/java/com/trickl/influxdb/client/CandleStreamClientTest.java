package com.trickl.influxdb.client;

import com.influxdb.client.InfluxDBClient;
import com.trickl.influxdb.config.InfluxDbConfiguration;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"unittest"})
@SpringBootTest(classes = InfluxDbConfiguration.class)
public class CandleStreamClientTest {

  @Mock private InfluxDBClient influxDb;

  private InfluxDbAdapter influxDbClient;

  private CandleStreamClient candleStreamClient;

  @BeforeEach
  private void setup() {}

  @AfterEach
  private void shutdown() throws IOException, InterruptedException {}

  @Test
  public void testGet() throws IOException, InterruptedException {}
}
