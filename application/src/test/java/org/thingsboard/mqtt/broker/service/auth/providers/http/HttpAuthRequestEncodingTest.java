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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.thingsboard.mqtt.broker.common.data.security.http.HttpMqttAuthProviderConfiguration;
import org.thingsboard.mqtt.broker.common.util.JacksonUtil;
import org.thingsboard.mqtt.broker.dao.client.provider.MqttAuthProviderService;
import org.thingsboard.mqtt.broker.service.auth.AuthorizationRuleService;
import org.thingsboard.mqtt.broker.service.auth.providers.AuthContext;
import org.thingsboard.mqtt.broker.service.auth.providers.AuthStatus;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Exercises the provider and real HTTP request, including GET query conversion. */
@RunWith(Parameterized.class)
public class HttpAuthRequestEncodingTest {
    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<Object[]> cases() {
        var cases = new ArrayList<Object[]>();
        for (String method : List.of("POST", "GET")) {
            for (String value : List.of("ordinary", "\\u0071ualification", "${username}-${password}",
                    "quote\"and\\slash", "line\n\ttab\r", "عربي-😀", "x\",\"injected\":true,\"x\":\"", "")) {
                cases.add(new Object[]{method, value});
            }
        }
        return cases;
    }

    private final String method;
    private final String value;
    private HttpServer server;
    private HttpMqttClientAuthProvider provider;
    private final AtomicReference<JsonNode> received = new AtomicReference<>();

    public HttpAuthRequestEncodingTest(String method, String value) {
        this.method = method;
        this.value = value;
    }

    @Before
    public void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth", exchange -> {
            try {
                if (method.equals("GET")) {
                    var values = JacksonUtil.newObjectNode();
                    for (String pair : exchange.getRequestURI().getRawQuery().split("&")) {
                        var fields = pair.split("=", 2);
                        values.put(URLDecoder.decode(fields[0], StandardCharsets.UTF_8),
                                URLDecoder.decode(fields.length == 2 ? fields[1] : "", StandardCharsets.UTF_8));
                    }
                    received.set(values);
                } else {
                    received.set(JacksonUtil.toJsonNode(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                }
            } catch (Exception ignored) {
                received.set(null);
            }
            byte[] response = "{\"result\":\"failure\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        var config = HttpMqttAuthProviderConfiguration.defaultConfiguration();
        config.setRestEndpointUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/auth");
        config.setRequestMethod(method);
        config.setRequestBody("{\"clientId\":\"${clientId}\",\"username\":\"${username}\",\"password\":\"${password}\","
                + "\"commonName\":\"${commonName}\",\"literal\":\"${unknown}\",\"repeated\":\"prefix:${clientId}:${clientId}\"}");
        provider = new HttpMqttClientAuthProvider(mock(MqttAuthProviderService.class), mock(AuthorizationRuleService.class));
        provider.onProviderUpdate(true, config);
    }

    @After
    public void stop() {
        if (provider != null) provider.destroy();
        if (server != null) server.stop(0);
    }

    @Test
    public void httpCallerPreservesExactValuesAndNeverReinterpretsInsertedPlaceholders() {
        var context = new AuthContext(value, value, value.getBytes(StandardCharsets.UTF_8), null);
        assertThat(provider.authenticate(context).getStatus()).isEqualTo(AuthStatus.FAILURE);
        JsonNode body = received.get();
        assertThat(body).isNotNull();
        assertThat(body.size()).isEqualTo(6);
        for (String field : List.of("clientId", "username", "password")) {
            assertThat(body.path(field).asText()).isEqualTo(value);
        }
        assertThat(body.path("commonName").asText()).isEmpty();
        assertThat(body.path("literal").asText()).isEqualTo("${unknown}");
        assertThat(body.path("repeated").asText()).isEqualTo("prefix:" + value + ":" + value);
    }
}
