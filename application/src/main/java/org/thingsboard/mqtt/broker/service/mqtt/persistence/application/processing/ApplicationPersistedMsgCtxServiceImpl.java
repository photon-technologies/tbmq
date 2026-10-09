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

import com.google.common.collect.Streams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.common.data.ApplicationMsgInfo;
import org.thingsboard.mqtt.broker.common.data.ApplicationMessageId;
import org.thingsboard.mqtt.broker.common.data.ApplicationSessionCtx;
import org.thingsboard.mqtt.broker.dao.client.application.ApplicationSessionCtxService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class ApplicationPersistedMsgCtxServiceImpl implements ApplicationPersistedMsgCtxService {

    private final ApplicationSessionCtxService sessionCtxService;

    @Override
    public ApplicationPersistedMsgCtx loadPersistedMsgCtx(String clientId) {
        if (log.isTraceEnabled()) {
            log.trace("[{}] Loading persisted messages context.", clientId);
        }
        ApplicationSessionCtx applicationSessionCtx = findApplicationSessionCtx(clientId);
        if (applicationSessionCtx == null) {
            return new ApplicationPersistedMsgCtx(Collections.emptyMap(), Collections.emptyMap());
        }
        Map<ApplicationMessageId, Integer> publishMsgIds = getPendingMsgsFromApplicationCtx(applicationSessionCtx.getPublishMsgInfos());
        Map<ApplicationMessageId, Integer> pubRelMsgIds = getPendingMsgsFromApplicationCtx(applicationSessionCtx.getPubRelMsgInfos());
        return new ApplicationPersistedMsgCtx(publishMsgIds, pubRelMsgIds);
    }

    private ApplicationSessionCtx findApplicationSessionCtx(String clientId) {
        return sessionCtxService.findApplicationSessionCtx(clientId).orElse(null);
    }

    private Map<ApplicationMessageId, Integer> getPendingMsgsFromApplicationCtx(Collection<ApplicationMsgInfo> applicationSessionCtx) {
        return applicationSessionCtx.stream()
                .collect(Collectors.toMap(info -> {
                    if (info.getMessageId() == null) {
                        throw new IllegalStateException("Unscoped legacy application context requires an explicit drained migration");
                    }
                    return info.getMessageId();
                }, ApplicationMsgInfo::getPacketId, (left, right) -> {
                    if (!left.equals(right)) {
                        throw new IllegalStateException("Conflicting packet IDs for one application record");
                    }
                    return left;
                }));
    }

    @Override
    public void saveContext(String clientId, ApplicationPackProcessingCtx processingContext, ApplicationPersistedMsgCtx restored) {
        if (log.isTraceEnabled()) {
            log.trace("[{}] Executing save application session context.", clientId);
        }
        if (restored == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No pack processing context found.", clientId);
            }
            return;
        }
        // Retain loaded packets that have not been polled yet. An immediate second
        // disconnect must not replace the durable snapshot with an empty active pack.
        Map<ApplicationMessageId, Integer> publishes = new HashMap<>(restored.getPublishMsgIds());
        Map<ApplicationMessageId, Integer> pubrels = new HashMap<>(restored.getPubRelMsgIds());
        if (processingContext != null) {
            toPublishMsgStream(processingContext).forEach(msg -> {
                publishes.put(msg.getMessageId(), msg.getPacketId());
                pubrels.remove(msg.getMessageId());
            });
            toPubRelMsgStream(processingContext).forEach(msg -> {
                pubrels.put(msg.getMessageId(), msg.getPacketId());
                publishes.remove(msg.getMessageId());
            });
        }
        ApplicationSessionCtx sessionCtx = buildApplicationSessionCtx(clientId,
                toMessageInfos(publishes), toMessageInfos(pubrels));
        if (log.isTraceEnabled()) {
            log.trace("[{}] Saving application session context - {}.", clientId, sessionCtx);
        }
        sessionCtxService.saveApplicationSessionCtx(sessionCtx);
    }

    private Stream<PersistedPublishMsg> toPublishMsgStream(ApplicationPackProcessingCtx processingContext) {
        return processingContext.getPublishPendingMsgMap().values().stream();
    }

    private Stream<PersistedPubRelMsg> toPubRelMsgStream(ApplicationPackProcessingCtx processingContext) {
        return Streams.concat(
                processingContext.getPubRelPendingMsgMap().values().stream(),
                processingContext.getPubRelMsgCtx().getPubRelMessagesToDeliver().stream()
        );
    }

    private List<ApplicationMsgInfo> toMessageInfos(Map<ApplicationMessageId, Integer> packets) {
        return packets.entrySet().stream()
                .map(entry -> new ApplicationMsgInfo(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private ApplicationSessionCtx buildApplicationSessionCtx(String clientId,
                                                             Collection<ApplicationMsgInfo> publishMsgInfos,
                                                             Collection<ApplicationMsgInfo> pubRelMsgInfos) {
        return ApplicationSessionCtx.builder()
                .clientId(clientId)
                .lastUpdatedTime(System.currentTimeMillis())
                .publishMsgInfos(publishMsgInfos)
                .pubRelMsgInfos(pubRelMsgInfos)
                .build();
    }

    @Override
    public void clearContext(String clientId) {
        if (log.isTraceEnabled()) {
            log.trace("[{}] Clearing application session context.", clientId);
        }
        try {
            sessionCtxService.deleteApplicationSessionCtx(clientId);
        } catch (EmptyResultDataAccessException noDataException) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No session for clientId.", clientId);
            }
        } catch (Exception e) {
            log.warn("[{}] Failed to clear application session context.", clientId, e);
        }
    }
}
