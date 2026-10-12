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
package org.thingsboard.mqtt.broker.queue.provider;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.thingsboard.mqtt.broker.queue.kafka.settings.ClientSessionEventKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.ClientSessionEventResponseKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.TbKafkaConsumerSettings;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class KafkaClientSessionEventQueueFactoryTest {
    @Test
    void firstRequestMustNotBeSkippedBeforeInitialAssignment() {
        assertInitialPosition(false, false);
    }

    @Test
    void firstResponseMustNotBeSkippedBeforeInitialAssignment() {
        assertInitialPosition(true, false);
    }

    @Test
    void latestOverrideCannotDiscardPendingRequests() {
        assertInitialPosition(false, true);
    }

    @Test
    void latestOverrideCannotDiscardPendingResponses() {
        assertInitialPosition(true, true);
    }

    private void assertInitialPosition(boolean response, boolean overrideLatest) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        if (overrideLatest) props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        ClientSessionEventKafkaSettings requests = mock(ClientSessionEventKafkaSettings.class);
        ClientSessionEventResponseKafkaSettings responses = mock(ClientSessionEventResponseKafkaSettings.class);
        when(requests.getKafkaTopic()).thenReturn("test.session.request");
        when(requests.getTopicProperties()).thenReturn("partitions:1;replication.factor:1");
        when(responses.getKafkaTopicPrefix()).thenReturn("test.session.response");
        when(responses.getTopicProperties()).thenReturn("partitions:1;replication.factor:1");
        KafkaClientSessionEventQueueFactory factory = new KafkaClientSessionEventQueueFactory(requests, responses);
        factory.consumerSettings = mock(TbKafkaConsumerSettings.class);
        when(factory.consumerSettings.toProps(any())).thenReturn(props);
        factory.kafkaPrefix = "test.";
        factory.init();
        AtomicReference<Properties> actual = new AtomicReference<>();
        try (MockedConstruction<KafkaConsumer> ignored = mockConstruction(KafkaConsumer.class,
                (consumer, context) -> actual.set((Properties) context.arguments().get(0)))) {
            if (response) factory.createEventResponseConsumer("cold-start");
            else factory.createEventConsumer("cold-start");
        }
        assertNotNull(actual.get());
        assertEquals(response ? "test.client-session-event-response-consumer-group-cold-start"
                : "test.client-session-event-consumer-group", actual.get().getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("earliest", new ConsumerConfig(actual.get()).getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }
}
