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
package org.thingsboard.mqtt.broker.common.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.thingsboard.mqtt.broker.common.data.security.MqttAuthProvider;
import org.thingsboard.mqtt.broker.common.data.security.MqttClientCredentials;
import org.thingsboard.mqtt.broker.common.data.security.MqttAuthProviderType;
import org.thingsboard.mqtt.broker.common.data.ws.WebSocketConnection;
import org.thingsboard.mqtt.broker.common.data.ws.WebSocketConnectionConfiguration;
import org.thingsboard.mqtt.broker.common.data.JavaSerDesUtil;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JacksonWireContractTest {

    @Test
    void integrationStatusRemainsOnJsonWire() {
        var integration = new org.thingsboard.mqtt.broker.common.data.integration.Integration();
        var status = JacksonUtil.newObjectNode().put("connected", true);
        integration.setStatus(status);

        var wire = JacksonUtil.toJsonNode(JacksonUtil.toString(integration));
        assertThat(wire.get("status")).isEqualTo(status);
        var restored = JacksonUtil.fromString(JacksonUtil.toString(integration),
                org.thingsboard.mqtt.broker.common.data.integration.Integration.class);
        assertThat(restored.getStatus()).isEqualTo(status);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void persistedMessageKeepsRetainFlagAndMqttProperties(boolean retained) {
        var properties = new io.netty.handler.codec.mqtt.MqttProperties();
        var original = new org.thingsboard.mqtt.broker.common.data.DevicePublishMsg(
                "qualification-device", "device/state", DEVICE_TS, 1, 42,
                org.thingsboard.mqtt.broker.common.data.PersistedPacketType.PUBLISH,
                new byte[]{1, 2, 3}, properties, retained);
        original.setMsgExpiryInterval(60);
        original.setContentType("application/json");
        original.setResponseTopic("device/reply");
        original.setCorrelationData(new byte[]{4, 5, 6});
        original.setSubscriptionIds(List.of(2, 3));
        var encoded = JacksonUtil.toBytes(original);
        var wire = HTTP_MAPPER.readTree(encoded);
        assertThat(wire.has("retained")).isTrue();
        assertThat(wire.has("isRetained")).isFalse();
        var restored = JacksonUtil.fromBytes(encoded, org.thingsboard.mqtt.broker.common.data.DevicePublishMsg.class);
        assertThat(restored.getProperties()).isNotNull();
        assertThat(restored.isRetained()).isEqualTo(retained);
        assertThat(restored.getPayload()).containsExactly(1, 2, 3);
        assertThat(restored.getCorrelationData()).containsExactly(4, 5, 6);
        assertThat(HTTP_MAPPER.readTree(JacksonUtil.toBytes(restored))).isEqualTo(wire);
    }


    @Test
    void sparseSslConfigurationKeepsSafeCertificateValidationDefault() {
        var config = JacksonUtil.fromString("{\"type\":\"X_509\",\"clientAuthType\":\"CLIENT_AUTH_REQUESTED\"}",
                org.thingsboard.mqtt.broker.common.data.security.ssl.SslMqttAuthProviderConfiguration.class);
        assertThat(config.isSkipValidityCheckForClientCert()).isFalse();
        assertThatThrownBy(() -> JacksonUtil.fromString(
                "{\"type\":\"X_509\",\"skipValidityCheckForClientCert\":null}",
                org.thingsboard.mqtt.broker.common.data.security.ssl.SslMqttAuthProviderConfiguration.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sparseHttpConfigurationKeepsOptionalNumericDefaults() {
        var config = JacksonUtil.fromString("{\"type\":\"HTTP\",\"restEndpointUrl\":\"https://auth.example.invalid\",\"requestMethod\":\"POST\",\"requestBody\":\"{}\"}",
                org.thingsboard.mqtt.broker.common.data.security.http.HttpMqttAuthProviderConfiguration.class);
        assertThat(config.getReadTimeoutMs()).isZero();
        assertThat(config.getMaxParallelRequestsCount()).isZero();
        assertThatThrownBy(() -> JacksonUtil.fromString("{\"type\":\"HTTP\",\"readTimeoutMs\":null}",
                org.thingsboard.mqtt.broker.common.data.security.http.HttpMqttAuthProviderConfiguration.class))
                .isInstanceOf(IllegalArgumentException.class);
    }



    private static final JsonMapper HTTP_MAPPER = JsonMapper.builder().build();
    private static final long DEVICE_TS = 1791813600123456L;

    @ParameterizedTest
    @EnumSource(MqttAuthProviderType.class)
    void providerConfigurationStaysPolymorphicAcrossJsonBoundary(MqttAuthProviderType type) {
        MqttAuthProvider provider = MqttAuthProvider.defaultAuthProvider(type, true);
        ObjectNode extra = JacksonUtil.newObjectNode();
        extra.put("device_ts", DEVICE_TS);
        extra.putArray("paths").add("0/6/0").add("0/8/0");
        provider.setAdditionalInfo(extra);

        byte[] encoded = JacksonUtil.toBytes(provider);
        JsonNode wire = HTTP_MAPPER.readTree(encoded);
        assertThat(wire.get("type").asString()).isEqualTo(type.name());
        assertThat(wire.get("configuration").isObject()).isTrue();
        assertThat(wire.get("additionalInfo")).isEqualTo(extra);
        assertThat(wire.has("additionalInfoBytes")).isFalse();

        MqttAuthProvider restored = HTTP_MAPPER.readValue(encoded, MqttAuthProvider.class);
        assertThat(restored.getType()).isEqualTo(type);
        assertThat(restored.getConfiguration().getClass()).isEqualTo(provider.getConfiguration().getClass());
        assertThat(HTTP_MAPPER.readTree(JacksonUtil.toBytes(restored))).isEqualTo(wire);
    }

    @Test
    void credentialAdditionalInfoSurvivesJavaSerialization() {
        MqttClientCredentials provider = new MqttClientCredentials();
        ObjectNode info = JacksonUtil.newObjectNode().put("device_ts", DEVICE_TS).put("label", "غرفة القاهرة");
        provider.setAdditionalInfo(info);
        MqttClientCredentials restored = JavaSerDesUtil.decode(JavaSerDesUtil.encode(provider));
        assertThat(restored.getAdditionalInfo()).isEqualTo(info);
        assertThat(restored.getAdditionalInfo().get("device_ts").longValue()).isEqualTo(DEVICE_TS);
    }

    @Test
    void websocketConfigurationSurvivesJavaSerialization() {
        WebSocketConnection connection = new WebSocketConnection();
        WebSocketConnectionConfiguration config = new WebSocketConnectionConfiguration("qualification-client");
        config.setUrl("wss://broker.example.invalid/mqtt");
        config.setCleanStart(true);
        config.setKeepAlive(60);
        connection.setConfiguration(config);
        WebSocketConnection restored = JavaSerDesUtil.decode(JavaSerDesUtil.encode(connection));
        assertThat(JacksonUtil.valueToTree(restored.getConfiguration())).isEqualTo(JacksonUtil.valueToTree(config));
        assertThat(HTTP_MAPPER.valueToTree(restored).has("configurationBytes")).isFalse();
    }

    @Test
    void nestedTreePreservesMicrosecondsUnicodeNullAndArrays() {
        String input = "{\"device_ts\":1791813600123456,\"label\":\"غرفة\",\"values\":[true,null,3.125],\"nested\":{\"on\":false}}";
        JsonNode expected = HTTP_MAPPER.readTree(input);
        JsonNode restored = JacksonUtil.fromBytes(JacksonUtil.toBytes(JacksonUtil.toJsonNode(input)));
        assertThat(restored).isEqualTo(expected);
        assertThat(restored.get("device_ts").longValue()).isEqualTo(DEVICE_TS);
        StringWriter out = new StringWriter();
        JacksonUtil.writeValue(out, restored);
        assertThat(JacksonUtil.fromReader(new StringReader(out.toString()), JsonNode.class)).isEqualTo(expected);
    }

    @Test
    void genericCollectionsKeepTheirDeclaredElementTypes() {
        Map<String, List<Long>> restored = JacksonUtil.fromString("{\"timestamps\":[1791813600123456]}", new TypeReference<>() {});
        assertThat(restored.get("timestamps")).containsExactly(DEVICE_TS);
    }

    @Test
    void malformedJsonRemainsAnInvalidArgument() {
        assertThatThrownBy(() -> JacksonUtil.toJsonNode("{broken")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void trailingDocumentIsRejectedAtTheBoundary() {
        assertThatThrownBy(() -> JacksonUtil.fromString("{} {}", Map.class)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullHandlingRemainsExplicit() {
        assertThat(JacksonUtil.toString(null)).isNull();
        assertThat(JacksonUtil.toBytes(null)).isNull();
        assertThat(JacksonUtil.toJsonNode(null)).isNull();
        assertThat(JacksonUtil.fromString(null, Map.class)).isNull();
    }
}
