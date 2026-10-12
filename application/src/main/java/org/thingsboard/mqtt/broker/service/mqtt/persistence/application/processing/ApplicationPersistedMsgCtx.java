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

import lombok.Getter;
import org.thingsboard.mqtt.broker.common.data.ApplicationMessageId;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@Getter
public class ApplicationPersistedMsgCtx {
    private final Map<ApplicationMessageId, Integer> publishMsgIds;
    private final Map<ApplicationMessageId, Integer> pubRelMsgIds;
    private final Map<Integer, ApplicationMessageId> reservedPacketIds = new ConcurrentHashMap<>();

    public ApplicationPersistedMsgCtx(Map<ApplicationMessageId, Integer> publishes,
                                     Map<ApplicationMessageId, Integer> pubrels) {
        this.publishMsgIds = new ConcurrentHashMap<>(publishes);
        this.pubRelMsgIds = new ConcurrentHashMap<>(pubrels);
        Stream.concat(publishes.entrySet().stream(), pubrels.entrySet().stream()).forEach(entry -> {
            int packetId = entry.getValue();
            if (packetId < 1 || packetId > 65535 || reservedPacketIds.putIfAbsent(packetId, entry.getKey()) != null) {
                throw new IllegalStateException("Invalid or multiply assigned persisted packet ID");
            }
        });
    }

    public ApplicationPersistedMsgCtx() {
        this(Collections.emptyMap(), Collections.emptyMap());
    }

    public Integer getMsgPacketId(ApplicationMessageId messageId) {
        // A miss on another stream must not erase this stream's pending packets.
        return publishMsgIds.get(messageId);
    }

    public boolean isPacketIdReserved(int packetId) {
        return reservedPacketIds.containsKey(packetId);
    }

    public void advanceToPubRel(int packetId) {
        ApplicationMessageId key = reservedPacketIds.get(packetId);
        if (key != null && publishMsgIds.remove(key, packetId)) {
            pubRelMsgIds.put(key, packetId);
        }
    }

    public void releasePacketId(int packetId) {
        ApplicationMessageId key = reservedPacketIds.remove(packetId);
        if (key != null) {
            publishMsgIds.remove(key, packetId);
            pubRelMsgIds.remove(key, packetId);
        }
    }

    public int getLastPacketId() {
        return reservedPacketIds.keySet().stream().max(Integer::compareTo).orElse(0);
    }
}
