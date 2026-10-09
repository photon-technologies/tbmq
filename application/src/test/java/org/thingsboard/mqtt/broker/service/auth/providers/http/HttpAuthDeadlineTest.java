/**
 * Copyright © 2016-2026 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.mqtt.broker.service.auth.providers.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import org.thingsboard.mqtt.broker.common.data.security.http.HttpAuthCallback;
import org.thingsboard.mqtt.broker.common.data.security.http.HttpMqttAuthProviderConfiguration;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A read-idle timeout cannot bound a response that keeps sending partial JSON. */
public class HttpAuthDeadlineTest {
    @Test
    public void nonpositiveDeadlineMustRejectConfiguration() {
        for (int timeout : new int[]{0, -1}) {
            var config = HttpMqttAuthProviderConfiguration.defaultConfiguration();
            config.setReadTimeoutMs(timeout);
            assertThatThrownBy(() -> new HttpAuthClient(config)).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    public void partialResponseMustExpireOnceAndReleasePermitForRecovery() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var executor = Executors.newCachedThreadPool();
        var hold = new AtomicBoolean(false);
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        server.setExecutor(executor);
        server.createContext("/auth", exchange -> {
            try {
                exchange.getRequestBody().readAllBytes();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, 0);
                if (hold.get()) {
                    entered.countDown();
                    while (!release.await(50, TimeUnit.MILLISECONDS)) {
                        exchange.getResponseBody().write(' ');
                        exchange.getResponseBody().flush();
                    }
                }
                exchange.getResponseBody().write("{\"result\":\"success\"}".getBytes(StandardCharsets.UTF_8));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (java.io.IOException cancelled) {
                // The deadline must cancel the unfinished HTTP response.
            } finally {
                exchange.close();
            }
        });
        server.start();
        var config = HttpMqttAuthProviderConfiguration.defaultConfiguration();
        config.setRestEndpointUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/auth");
        config.setMaxParallelRequestsCount(1);
        config.setReadTimeoutMs(1500);
        var client = new HttpAuthClient(config);
        try {
            var warm = new Result();
            client.processMessage("warm", "{}", warm);
            assertThat(warm.done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(warm.successes.get()).isEqualTo(1);
            hold.set(true);
            var timed = new Result();
            client.processMessage("held", "{}", timed);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(timed.done.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(timed.failures.get()).isEqualTo(1);
            assertThat(timed.successes.get()).isZero();
            assertThat(client.getSemaphore().availablePermits()).isEqualTo(1);
            hold.set(false);
            release.countDown();
            var recovered = new Result();
            client.processMessage("recovered", "{}", recovered);
            assertThat(recovered.done.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(recovered.successes.get()).isEqualTo(1);
            assertThat(recovered.failures.get()).isZero();
            assertThat(timed.failures.get()).isEqualTo(1);
            assertThat(timed.successes.get()).isZero();
            assertThat(client.getSemaphore().availablePermits()).isEqualTo(1);
        } finally {
            release.countDown();
            client.destroy();
            server.stop(0);
            executor.shutdownNow();
        }
    }

    private static class Result implements HttpAuthCallback {
        final AtomicInteger successes = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        @Override public void onSuccess(ResponseEntity<JsonNode> result) { successes.incrementAndGet(); done.countDown(); }
        @Override public void onFailure(Throwable error) { failures.incrementAndGet(); done.countDown(); }
    }
}
