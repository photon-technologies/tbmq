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
package org.thingsboard.mqtt.broker.service.test.util;

import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttClientException;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestUtilsCleanupTest {
    @Test
    void closesClientThatNeverConnectedWithoutDisconnectingIt() throws Exception {
        MqttClient client = mock(MqttClient.class);

        TestUtils.disconnectAndCloseClient(client);

        verify(client, never()).disconnect();
        verify(client).close();
    }

    @Test
    void disconnectsConnectedClientBeforeClosingIt() throws Exception {
        MqttClient client = mock(MqttClient.class);
        when(client.isConnected()).thenReturn(true);

        TestUtils.disconnectAndCloseClient(client);

        var order = inOrder(client);
        order.verify(client).disconnect();
        order.verify(client).close();
    }

    @Test
    void attemptsCloseAndPropagatesUnexpectedDisconnectFailure() throws Exception {
        MqttClient client = mock(MqttClient.class);
        when(client.isConnected()).thenReturn(true);
        MqttException failure = new MqttException(MqttClientException.REASON_CODE_WRITE_TIMEOUT);
        doThrow(failure).when(client).disconnect();

        assertSame(failure, assertThrows(MqttException.class,
                () -> TestUtils.disconnectAndCloseClient(client)));
        verify(client).close();
    }

    @Test
    void preservesBothFailuresWhenDisconnectAndCloseFail() throws Exception {
        MqttClient client = mock(MqttClient.class);
        when(client.isConnected()).thenReturn(true);
        MqttException disconnectFailure = new MqttException(MqttClientException.REASON_CODE_WRITE_TIMEOUT);
        MqttException closeFailure = new MqttException(MqttClientException.REASON_CODE_CLIENT_CONNECTED);
        doThrow(disconnectFailure).when(client).disconnect();
        doThrow(closeFailure).when(client).close();

        MqttException actual = assertThrows(MqttException.class,
                () -> TestUtils.disconnectAndCloseClient(client));

        assertSame(disconnectFailure, actual);
        assertArrayEquals(new Throwable[] {closeFailure}, actual.getSuppressed());
    }

    @Test
    void propagatesCloseFailureWhenThereIsNoDisconnectFailure() throws Exception {
        MqttClient client = mock(MqttClient.class);
        MqttException closeFailure = new MqttException(MqttClientException.REASON_CODE_CLIENT_CLOSED);
        doThrow(closeFailure).when(client).close();

        assertSame(closeFailure, assertThrows(MqttException.class,
                () -> TestUtils.disconnectAndCloseClient(client)));
    }

}
