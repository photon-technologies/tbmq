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
import org.thingsboard.mqtt.broker.queue.kafka.settings.PublishMsgKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.ApplicationPersistenceMsgKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.ApplicationSharedTopicMsgKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.TbKafkaConsumerSettings;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class KafkaPublishMsgQueueFactoryTest {

    @Test
    void newConsumerGroupMustReadAlreadyAcknowledgedMessages() {
        assertInitialPosition(new Properties());
    }

    @Test
    void unsafeLatestOverrideCannotSkipAlreadyAcknowledgedMessages() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        assertInitialPosition(properties);
    }

    @Test
    void sharedReconnectWithoutFirstCommitMustReplayPendingRecords() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        KafkaApplicationPersistenceMsgQueueFactory factory = new KafkaApplicationPersistenceMsgQueueFactory(
                mock(ApplicationPersistenceMsgKafkaSettings.class), mock(ApplicationSharedTopicMsgKafkaSettings.class));
        factory.consumerSettings = mock(TbKafkaConsumerSettings.class);
        when(factory.consumerSettings.toProps(any())).thenReturn(props);
        factory.kafkaPrefix = "test.";
        AtomicReference<Properties> actual = new AtomicReference<>();
        try (MockedConstruction<KafkaConsumer> ignored = mockConstruction(KafkaConsumer.class,
                (consumer, context) -> actual.set((Properties) context.arguments().get(0)))) {
            factory.createConsumerForSharedTopic("shared-topic", "shared-group", "reconnect");
        }
        assertNotNull(actual.get());
        assertEquals("earliest", new ConsumerConfig(actual.get()).getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    private void assertInitialPosition(Properties properties) {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        PublishMsgKafkaSettings settings = mock(PublishMsgKafkaSettings.class);
        when(settings.getKafkaTopic()).thenReturn("tbmq.msg.all");
        when(settings.getTopicProperties()).thenReturn("partitions:1;replication.factor:1");
        KafkaPublishMsgQueueFactory factory = new KafkaPublishMsgQueueFactory(settings);
        factory.consumerSettings = mock(TbKafkaConsumerSettings.class);
        when(factory.consumerSettings.toProps(any())).thenReturn(properties);
        factory.kafkaPrefix = "test.";
        factory.init();
        AtomicReference<Properties> actual = new AtomicReference<>();
        // Exercise the real factory and builder, intercepting only the network client.
        try (MockedConstruction<KafkaConsumer> ignored = mockConstruction(KafkaConsumer.class,
                (consumer, context) -> actual.set((Properties) context.arguments().get(0)))) {
            factory.createConsumer("cold-start");
        }
        assertNotNull(actual.get());
        assertEquals("test.msg-all-consumer-group", actual.get().getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        // Resolve Kafka defaults too: absence means latest and must fail this guard.
        assertEquals("earliest", new ConsumerConfig(actual.get()).getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }
}
