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
package org.thingsboard.mqtt.broker.common.data;

/** A Kafka offset is unique only inside its application stream and partition. */
public record ApplicationMessageId(String stream, int partition, long offset) {
    public ApplicationMessageId {
        if (stream == null || partition < 0 || offset < 0) {
            throw new IllegalArgumentException("Complete application message identity required");
        }
    }

    public static ApplicationMessageId main(int partition, long offset) {
        return new ApplicationMessageId("", partition, offset);
    }
}
