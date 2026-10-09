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

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.thingsboard.mqtt.broker.common.data.client.credentials.PubSubAuthorizationRules;
import org.thingsboard.mqtt.broker.common.data.security.http.HttpMqttAuthProviderConfiguration;
import org.thingsboard.mqtt.broker.dao.client.provider.MqttAuthProviderService;
import org.thingsboard.mqtt.broker.service.auth.DefaultAuthorizationRuleService;
import org.thingsboard.mqtt.broker.service.auth.providers.AuthContext;
import org.thingsboard.mqtt.broker.service.auth.providers.AuthStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Actual HTTP replies through the provider; invalid verdicts must never grant defaults. */
@RunWith(Parameterized.class)
public class HttpAuthExplicitVerdictTest {
    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<Object[]> cases() {
        return List.of(
            new Object[]{"empty-200", "", 200, AuthStatus.FAILURE},
            new Object[]{"empty-204", "", 204, AuthStatus.FAILURE},
            new Object[]{"json-null", "null", 200, AuthStatus.FAILURE},
            new Object[]{"empty-object", "{}", 200, AuthStatus.FAILURE},
            new Object[]{"array", "[]", 200, AuthStatus.FAILURE},
            new Object[]{"missing-result", "{\"clientType\":\"DEVICE\"}", 200, AuthStatus.FAILURE},
            new Object[]{"null-result", "{\"result\":null}", 200, AuthStatus.FAILURE},
            new Object[]{"unknown-result", "{\"result\":\"allow\"}", 200, AuthStatus.FAILURE},
            new Object[]{"numeric-result", "{\"result\":1}", 200, AuthStatus.FAILURE},
            new Object[]{"unknown-type", "{\"result\":\"success\",\"clientType\":\"invalid\"}", 200, AuthStatus.FAILURE},
            new Object[]{"integration-type", "{\"result\":\"success\",\"clientType\":\"INTEGRATION\"}", 200, AuthStatus.FAILURE},
            new Object[]{"invalid-rule", "{\"result\":\"success\",\"authRules\":{\"pub\":[\"[\"]}}", 200, AuthStatus.FAILURE},
            new Object[]{"invalid-rules-object", "{\"result\":\"success\",\"authRules\":\"wrong\"}", 200, AuthStatus.FAILURE},
            new Object[]{"explicit-denial", "{\"result\":\"failure\"}", 200, AuthStatus.FAILURE},
            new Object[]{"explicit-skip", "{\"result\":\"skipped\"}", 200, AuthStatus.SKIPPED},
            new Object[]{"explicit-defaults", "{\"result\":\"success\"}", 200, AuthStatus.SUCCESS},
            new Object[]{"explicit-device", "{\"result\":\"success\",\"clientType\":\"DEVICE\",\"authRules\":{\"pub\":[],\"sub\":[]}}", 200, AuthStatus.SUCCESS},
            new Object[]{"explicit-application", "{\"result\":\"SuCcEsS\",\"clientType\":\"application\",\"authRules\":{\"pub\":[\"^fixture$\"],\"sub\":[]}}", 200, AuthStatus.SUCCESS}
        );
    }
    private final String name;
    private final String body;
    private final int status;
    private final AuthStatus expected;
    private HttpServer server;
    private HttpMqttClientAuthProvider provider;
    private DefaultAuthorizationRuleService rules;

    public HttpAuthExplicitVerdictTest(String name, String body, int status, AuthStatus expected) {
        this.name=name; this.body=body; this.status=status; this.expected=expected;
    }
    @Before
    public void start() throws Exception {
        server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/auth", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type","application/json");
            exchange.sendResponseHeaders(status, bytes.length==0 ? -1 : bytes.length);
            if(bytes.length>0) exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        var config=HttpMqttAuthProviderConfiguration.defaultConfiguration();
        config.setRestEndpointUrl("http://127.0.0.1:"+server.getAddress().getPort()+"/auth");
        config.setAuthRules(new PubSubAuthorizationRules(List.of(),List.of()));
        rules=new DefaultAuthorizationRuleService();
        provider=new HttpMqttClientAuthProvider(mock(MqttAuthProviderService.class),rules);
        provider.onProviderUpdate(true,config);
    }
    @After
    public void stop() {
        if(provider!=null) provider.destroy();
        if(server!=null) server.stop(0);
    }
    @Test
    public void requiresAnExplicitValidVerdict() {
        var response=provider.authenticate(new AuthContext("fixture",null,null,null));
        assertThat(response.getStatus()).as(name).isEqualTo(expected);
        if(expected==AuthStatus.SUCCESS) {
            assertThat(rules.isSubAuthorized("fixture",response.getAuthRulePatterns())).isFalse();
            assertThat(rules.isPubAuthorized("fixture","fixture",response.getAuthRulePatterns()))
                .isEqualTo(name.equals("explicit-application"));
        }
    }
}
