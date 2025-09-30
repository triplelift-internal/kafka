/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DistributedConfigGlobalBalanceTest {

    @Test
    public void testGlobalTaskBalanceConfigDefault() {
        Map<String, String> props = getMinimalConfig();
        DistributedConfig config = new DistributedConfig(props);

        // Default should be false
        assertFalse(config.globalTaskBalanceEnabled());
    }

    @Test
    public void testGlobalTaskBalanceConfigEnabled() {
        Map<String, String> props = getMinimalConfig();
        props.put(DistributedConfig.GLOBAL_TASK_BALANCE_ENABLED_CONFIG, "true");
        DistributedConfig config = new DistributedConfig(props);

        assertTrue(config.globalTaskBalanceEnabled());
    }

    @Test
    public void testGlobalTaskBalanceConfigDisabled() {
        Map<String, String> props = getMinimalConfig();
        props.put(DistributedConfig.GLOBAL_TASK_BALANCE_ENABLED_CONFIG, "false");
        DistributedConfig config = new DistributedConfig(props);

        assertFalse(config.globalTaskBalanceEnabled());
    }

    private Map<String, String> getMinimalConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(DistributedConfig.GROUP_ID_CONFIG, "test-group");
        props.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "test-config");
        props.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "test-offsets");
        props.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "test-status");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        return props;
    }
}