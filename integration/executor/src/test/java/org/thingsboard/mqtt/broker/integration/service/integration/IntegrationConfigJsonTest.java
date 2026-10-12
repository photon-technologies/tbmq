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
package org.thingsboard.mqtt.broker.integration.service.integration;

import org.junit.jupiter.api.Test;
import org.thingsboard.mqtt.broker.common.data.credentials.AnonymousCredentials;
import org.thingsboard.mqtt.broker.common.util.JacksonUtil;
import org.thingsboard.mqtt.broker.integration.service.integration.http.HttpIntegrationConfig;
import org.thingsboard.mqtt.broker.integration.service.integration.mqtt.MqttIntegrationConfig;
import org.thingsboard.mqtt.broker.integration.service.integration.kafka.KafkaIntegrationConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntegrationConfigJsonTest {
    @Test
    void httpOmittedOptionalFieldsKeepDefaults() {
        var config = JacksonUtil.fromString("{\"restEndpointUrl\":\"https://example.invalid\"}", HttpIntegrationConfig.class);
        assertThat(config.getRequestMethod()).isEqualTo("POST");
        assertThat(config.isSendOnlyMsgPayload()).isFalse();
        assertThat(config.getReadTimeoutMs()).isZero();
        assertThat(config.getCredentials()).isInstanceOf(AnonymousCredentials.class);
    }

    @Test
    void mqttOmittedOptionalFieldsKeepDefaults() {
        var config = JacksonUtil.fromString("{\"host\":\"broker.example.invalid\",\"port\":1883,\"topicName\":\"device/state\"}", MqttIntegrationConfig.class);
        assertThat(config.isSsl()).isFalse();
        assertThat(config.isRetained()).isFalse();
        assertThat(config.getCredentials()).isInstanceOf(AnonymousCredentials.class);
    }

    @Test
    void explicitNullPrimitivesRemainInvalidForEveryTransport() {
        for (var type : java.util.List.of(HttpIntegrationConfig.class, MqttIntegrationConfig.class, KafkaIntegrationConfig.class)) {
            assertThatThrownBy(() -> JacksonUtil.fromString("{\"sendOnlyMsgPayload\":null}", type))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
