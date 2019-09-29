package com.trickl.smarkets.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.influxdb.client.CandleStreamClient;
import com.trickl.influxdb.config.InfluxDbConfiguration;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.socket.sockjs.client.SockJsUrlInfo;
import org.springframework.web.socket.sockjs.transport.TransportType;

import lombok.extern.java.Log;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@ActiveProfiles({ "unittest" })
@SpringBootTest(classes = InfluxDbConfiguration.class)
@Log
public class CandleStreamClientTest {

  @Mock
  private InfluxDB influxDb;

  private final Mono<InfluxDB> influxDbConnection;

  private CandleStreamClient candleStreamClient;

  @BeforeEach
  private void setup() {
    candleStreamClient = new CandleStreamClient(influxDbConnection);
  }

  @AfterEach
  private void shutdown() throws IOException, InterruptedException {
  }

  @Test
  public void testGet() throws IOException, InterruptedException {

  }
}
