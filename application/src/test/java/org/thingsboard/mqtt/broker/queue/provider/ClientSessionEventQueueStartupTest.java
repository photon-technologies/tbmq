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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.thingsboard.mqtt.broker.gen.queue.ClientSessionEventProto;
import org.thingsboard.mqtt.broker.gen.queue.ClientSessionEventResponseProto;
import org.thingsboard.mqtt.broker.queue.TbQueueAdmin;
import org.thingsboard.mqtt.broker.queue.TbQueueControlledOffsetConsumer;
import org.thingsboard.mqtt.broker.queue.TbQueueMsg;
import org.thingsboard.mqtt.broker.queue.kafka.settings.ClientSessionEventKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.ClientSessionEventResponseKafkaSettings;
import org.thingsboard.mqtt.broker.queue.kafka.settings.TbKafkaConsumerSettings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClientSessionEventQueueStartupTest {
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.9.2"));

    @BeforeAll
    static void startKafka() {
        KAFKA.start();
    }

    @AfterAll
    static void stopKafka() {
        KAFKA.stop();
    }

    @Test
    void requestPublishedBeforeFirstPollSurvivesStartupAndCommittedRestart() throws Exception {
        verifyStartupAndRestart(false);
    }

    @Test
    void responsePublishedBeforeFirstPollSurvivesStartupAndCommittedRestart() throws Exception {
        verifyStartupAndRestart(true);
    }

    private void verifyStartupAndRestart(boolean response) throws Exception {
        String prefix = response ? "response-startup." : "request-startup.";
        String topic = prefix + (response ? "response.node" : "request");
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (AdminClient admin = AdminClient.create(producerProps)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        ClientSessionEventKafkaSettings requests = mock(ClientSessionEventKafkaSettings.class);
        ClientSessionEventResponseKafkaSettings responses = mock(ClientSessionEventResponseKafkaSettings.class);
        when(requests.getKafkaTopic()).thenReturn(topic);
        when(requests.getTopicProperties()).thenReturn("partitions:1;replication.factor:1");
        when(responses.getKafkaTopicPrefix()).thenReturn(prefix + "response");
        when(responses.getTopicProperties()).thenReturn("partitions:1;replication.factor:1");
        KafkaClientSessionEventQueueFactory factory = new KafkaClientSessionEventQueueFactory(requests, responses);
        factory.kafkaPrefix = prefix;
        factory.queueAdmin = mock(TbQueueAdmin.class); // Topic created by the real AdminClient above.
        factory.consumerSettings = mock(TbKafkaConsumerSettings.class);
        when(factory.consumerSettings.toProps(any())).thenAnswer(invocation -> {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            return props;
        });
        factory.init();
        byte[] value = response ? ClientSessionEventResponseProto.getDefaultInstance().toByteArray()
                : ClientSessionEventProto.getDefaultInstance().toByteArray();
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, "before-startup", value)).get(30, TimeUnit.SECONDS);
            consumeAndCommit(factory, response, "before-startup");
            producer.send(new ProducerRecord<>(topic, "after-restart", value)).get(30, TimeUnit.SECONDS);
            consumeAndCommit(factory, response, "after-restart");
        }
    }

    private void consumeAndCommit(KafkaClientSessionEventQueueFactory factory, boolean response, String key) {
        TbQueueControlledOffsetConsumer<? extends TbQueueMsg> consumer = response
                ? factory.createEventResponseConsumer("node") : factory.createEventConsumer("node");
        try {
            consumer.subscribe();
            List<String> keys = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (keys.isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(100).forEach(message -> keys.add(message.getKey()));
            }
            assertEquals(List.of(key), keys);
            consumer.commitSync();
        } finally {
            consumer.unsubscribeAndClose();
        }
    }
}
