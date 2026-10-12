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
package org.thingsboard.mqtt.broker.service.mqtt.persistence.application.processing;

import org.thingsboard.mqtt.broker.common.data.ApplicationMessageId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.thingsboard.mqtt.broker.common.data.ApplicationMsgInfo;
import org.thingsboard.mqtt.broker.common.data.ApplicationSessionCtx;
import org.thingsboard.mqtt.broker.dao.client.application.ApplicationSessionCtxService;
import org.thingsboard.mqtt.broker.service.mqtt.PublishMsg;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ApplicationPersistedMsgCtxServiceImplTest {

    static final String CLIENT_ID = "clientId";
    static final long TS = 1657202435000L;

    ApplicationSessionCtxService sessionCtxService;
    ApplicationPersistedMsgCtxServiceImpl applicationPersistedMsgCtxService;

    @Before
    public void setUp() throws Exception {
        sessionCtxService = mock(ApplicationSessionCtxService.class);
        applicationPersistedMsgCtxService = spy(new ApplicationPersistedMsgCtxServiceImpl(sessionCtxService));
    }

    @Test
    public void loadEmptyPersistedMsgCtxTest() {
        when(sessionCtxService.findApplicationSessionCtx(CLIENT_ID)).thenReturn(Optional.empty());

        ApplicationPersistedMsgCtx applicationPersistedMsgCtx = applicationPersistedMsgCtxService.loadPersistedMsgCtx(CLIENT_ID);
        Assert.assertTrue(applicationPersistedMsgCtx.getPublishMsgIds().isEmpty());
        Assert.assertTrue(applicationPersistedMsgCtx.getPubRelMsgIds().isEmpty());
    }

    @Test
    public void loadPersistedMsgCtxTest() {
        ApplicationSessionCtx applicationSessionCtx = buildApplicationSessionCtx();
        when(sessionCtxService.findApplicationSessionCtx(CLIENT_ID)).thenReturn(Optional.of(applicationSessionCtx));

        ApplicationPersistedMsgCtx actual = applicationPersistedMsgCtxService.loadPersistedMsgCtx(CLIENT_ID);
        ApplicationPersistedMsgCtx expected = new ApplicationPersistedMsgCtx(getPendingMsgIdsMap(), Map.of());
        Assert.assertEquals(expected.getPublishMsgIds(), actual.getPublishMsgIds());
        Assert.assertEquals(expected.getPubRelMsgIds(), actual.getPubRelMsgIds());
    }

    @Test
    public void equalOffsetsFromSharedQueuesMustNotDiscardPendingPackets() {
        // Merged shared-topic/partition contexts can carry the same Kafka offset.
        ApplicationSessionCtx saved = ApplicationSessionCtx.builder()
                .clientId(CLIENT_ID).lastUpdatedTime(TS)
                .publishMsgInfos(List.of(new ApplicationMsgInfo(new ApplicationMessageId("$share/photon/one", 0, 0), 101), new ApplicationMsgInfo(new ApplicationMessageId("$share/photon/two", 0, 0), 102)))
                .pubRelMsgInfos(List.of()).build();
        when(sessionCtxService.findApplicationSessionCtx(CLIENT_ID)).thenReturn(Optional.of(saved));
        ApplicationPersistedMsgCtx loaded = applicationPersistedMsgCtxService.loadPersistedMsgCtx(CLIENT_ID);
        Assert.assertEquals("Every unacknowledged packet must survive reconnect", 2, loaded.getPublishMsgIds().size());
    }

    @Test
    public void sameStreamDifferentPartitionsAndOtherStreamsRemainIndependent() {
        ApplicationMessageId a = new ApplicationMessageId("$share/photon/state", 0, 7);
        ApplicationMessageId b = new ApplicationMessageId("$share/photon/state", 1, 7);
        ApplicationMessageId c = new ApplicationMessageId("$share/another/state", 0, 7);
        ApplicationPersistedMsgCtx context = new ApplicationPersistedMsgCtx(Map.of(a, 65535, b, 1), Map.of(c, 2));
        Assert.assertEquals(Integer.valueOf(65535), context.getMsgPacketId(a));
        Assert.assertNull(context.getMsgPacketId(ApplicationMessageId.main(0, 7)));
        Assert.assertEquals(Integer.valueOf(1), context.getMsgPacketId(b));
        Assert.assertTrue(context.isPacketIdReserved(1));
        context.releasePacketId(65535);
        Assert.assertFalse(context.isPacketIdReserved(65535));
        Assert.assertNull(context.getMsgPacketId(a));
        Assert.assertEquals(Integer.valueOf(1), context.getMsgPacketId(b));
        Assert.assertTrue(context.isPacketIdReserved(2));
    }

    @Test(expected = IllegalStateException.class)
    public void conflictingPacketAssignmentsMustFailExplicitly() {
        ApplicationMessageId a = ApplicationMessageId.main(0, 1);
        ApplicationMessageId b = ApplicationMessageId.main(0, 2);
        new ApplicationPersistedMsgCtx(Map.of(a, 7), Map.of(b, 7));
    }

    @Test(expected = IllegalStateException.class)
    public void legacyPendingRecordsCannotBeSilentlyDroppedOrGuessed() {
        ApplicationSessionCtx saved = ApplicationSessionCtx.builder()
                .clientId(CLIENT_ID).lastUpdatedTime(TS)
                .publishMsgInfos(List.of(new ApplicationMsgInfo(null, 101)))
                .pubRelMsgInfos(List.of()).build();
        when(sessionCtxService.findApplicationSessionCtx(CLIENT_ID)).thenReturn(Optional.of(saved));
        applicationPersistedMsgCtxService.loadPersistedMsgCtx(CLIENT_ID);
    }

    private ApplicationSessionCtx buildApplicationSessionCtx() {
        return ApplicationSessionCtx.builder()
                .clientId(CLIENT_ID)
                .lastUpdatedTime(TS)
                .publishMsgInfos(getApplicationMsgInfos())
                .pubRelMsgInfos(List.of())
                .build();
    }

    private List<ApplicationMsgInfo> getApplicationMsgInfos() {
        return List.of(newApplicationMsgInfo(100L, 100), newApplicationMsgInfo(200L, 200));
    }

    private ApplicationMsgInfo newApplicationMsgInfo(long offset, int packetId) {
        return new ApplicationMsgInfo(ApplicationMessageId.main(0, offset), packetId);
    }

    private Map<ApplicationMessageId, Integer> getPendingMsgIdsMap() {
        return Map.of(ApplicationMessageId.main(0, 100L), 100, ApplicationMessageId.main(0, 200L), 200);
    }

    @Test
    public void saveContextTest() {
        ApplicationPackProcessingCtx applicationPackProcessingCtx = mock(ApplicationPackProcessingCtx.class);

        when(applicationPackProcessingCtx.getPublishPendingMsgMap())
                .thenReturn(new ConcurrentHashMap<>(Map.of(1, new PersistedPublishMsg(buildPubMsg(), ApplicationMessageId.main(0, 100), false))));
        when(applicationPackProcessingCtx.getPubRelPendingMsgMap())
                .thenReturn(new ConcurrentHashMap<>(Map.of(4, new PersistedPubRelMsg(4, ApplicationMessageId.main(0, 400)))));
        when(applicationPackProcessingCtx.getPubRelMsgCtx())
                .thenReturn(new ApplicationPubRelMsgCtx(Set.of(new PersistedPubRelMsg(2, ApplicationMessageId.main(0, 200)), new PersistedPubRelMsg(3, ApplicationMessageId.main(0, 300)))));

        applicationPersistedMsgCtxService.saveContext(CLIENT_ID, applicationPackProcessingCtx, new ApplicationPersistedMsgCtx());

        ArgumentCaptor<ApplicationSessionCtx> applicationSessionCtxCaptor = ArgumentCaptor.forClass(ApplicationSessionCtx.class);
        verify(sessionCtxService, times(1)).saveApplicationSessionCtx(applicationSessionCtxCaptor.capture());
        ApplicationSessionCtx applicationSessionCtx = applicationSessionCtxCaptor.getValue();

        Assert.assertEquals(CLIENT_ID, applicationSessionCtx.getClientId());
        Assert.assertTrue(applicationSessionCtx.getLastUpdatedTime() > 0);
        Assert.assertEquals(List.of(new ApplicationMsgInfo(ApplicationMessageId.main(0, 100), 1)), applicationSessionCtx.getPublishMsgInfos());
        Assert.assertTrue(
                applicationSessionCtx.getPubRelMsgInfos().containsAll(
                        List.of(new ApplicationMsgInfo(ApplicationMessageId.main(0, 400), 4),
                                new ApplicationMsgInfo(ApplicationMessageId.main(0, 200), 2),
                                new ApplicationMsgInfo(ApplicationMessageId.main(0, 300), 3)
                        ))
        );
    }

    @Test
    public void disconnectBeforeRepollMustPreserveLoadedSnapshot() {
        ApplicationMessageId key = new ApplicationMessageId("$share/test/state", 0, 0);
        ApplicationPersistedMsgCtx restored = new ApplicationPersistedMsgCtx(Map.of(key, 101), Map.of());
        applicationPersistedMsgCtxService.saveContext(CLIENT_ID, new ApplicationPackProcessingCtx(CLIENT_ID), restored);
        ArgumentCaptor<ApplicationSessionCtx> saved = ArgumentCaptor.forClass(ApplicationSessionCtx.class);
        verify(sessionCtxService).saveApplicationSessionCtx(saved.capture());
        Assert.assertEquals(List.of(new ApplicationMsgInfo(key, 101)), saved.getValue().getPublishMsgInfos());
    }

    @Test
    public void qos2TransitionAndTerminalAckMustNotResurrectPublish() {
        ApplicationMessageId key = new ApplicationMessageId("$share/test/state", 0, 0);
        ApplicationPersistedMsgCtx restored = new ApplicationPersistedMsgCtx(Map.of(key, 101), Map.of());
        restored.advanceToPubRel(101);
        Assert.assertNull(restored.getMsgPacketId(key));
        Assert.assertTrue(restored.isPacketIdReserved(101));
        applicationPersistedMsgCtxService.saveContext(CLIENT_ID, new ApplicationPackProcessingCtx(CLIENT_ID), restored);
        restored.releasePacketId(101);
        applicationPersistedMsgCtxService.saveContext(CLIENT_ID, new ApplicationPackProcessingCtx(CLIENT_ID), restored);
        ArgumentCaptor<ApplicationSessionCtx> saved = ArgumentCaptor.forClass(ApplicationSessionCtx.class);
        verify(sessionCtxService, times(2)).saveApplicationSessionCtx(saved.capture());
        Assert.assertTrue(saved.getAllValues().get(0).getPublishMsgInfos().isEmpty());
        Assert.assertEquals(List.of(new ApplicationMsgInfo(key, 101)), saved.getAllValues().get(0).getPubRelMsgInfos());
        Assert.assertTrue(saved.getAllValues().get(1).getPublishMsgInfos().isEmpty());
        Assert.assertTrue(saved.getAllValues().get(1).getPubRelMsgInfos().isEmpty());
    }

    @Test
    public void failedRestoreMustNotOverwriteDurableSnapshot() {
        applicationPersistedMsgCtxService.saveContext(CLIENT_ID, new ApplicationPackProcessingCtx(CLIENT_ID), null);
        verify(sessionCtxService, times(0)).saveApplicationSessionCtx(any());
    }

    private PublishMsg buildPubMsg() {
        return PublishMsg.builder()
                .packetId(1)
                .topicName("topic")
                .payload("data".getBytes())
                .qos(1)
                .isRetained(false)
                .isDup(false)
                .build();
    }

    @Test
    public void saveNullContextTest() {
        applicationPersistedMsgCtxService.saveContext(CLIENT_ID, null, null);
        verify(sessionCtxService, times(0)).saveApplicationSessionCtx(any());
    }

    @Test
    public void clearContextTest() {
        applicationPersistedMsgCtxService.clearContext(CLIENT_ID);
        verify(sessionCtxService, times(1)).deleteApplicationSessionCtx(eq(CLIENT_ID));
    }
}