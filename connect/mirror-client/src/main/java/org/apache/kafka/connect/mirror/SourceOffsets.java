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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SourceOffsets {
    public static final String CLUSTER_FIELD_KEY = "cluster";
    public static final String OFFSET_FIELD_KEY = "offset";
    public static final String RECORDS_FIELD_KEY = "records";
    public static final org.apache.kafka.common.protocol.types.Schema RECORD_SCHEMA =
            new org.apache.kafka.common.protocol.types.Schema(
                    new Field(CLUSTER_FIELD_KEY, Type.STRING),
                    new Field(OFFSET_FIELD_KEY, Type.INT64)
            );
    public static final org.apache.kafka.common.protocol.types.Schema SCHEMA =
            new org.apache.kafka.common.protocol.types.Schema(
                    new Field(RECORDS_FIELD_KEY, new ArrayOf(RECORD_SCHEMA))
            );

    private Map<String, Long> sourceOffsets;

    public SourceOffsets() {
        sourceOffsets = new HashMap<>();
    }

    public SourceOffsets(Map<String, Long> sourceOffsets) {
        this.sourceOffsets = sourceOffsets;
    }

    public Map<String, Long> sourceOffsets() {
        return sourceOffsets;
    }

    public ByteBuffer serialize() {
        Struct struct = new Struct(SCHEMA);
        Object[] records = new Object[sourceOffsets.size()];
        int i = 0;
        for (Map.Entry<String, Long> entry : sourceOffsets.entrySet()) {
            records[i++] = recordStruct(entry);
        }
        struct.set(RECORDS_FIELD_KEY, records);

        ByteBuffer buffer = ByteBuffer.allocate(SCHEMA.sizeOf(struct));
        SCHEMA.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public void deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Struct struct = SCHEMA.read(buffer);
        Object[] records = struct.getArray(RECORDS_FIELD_KEY);
        sourceOffsets = new HashMap<>();
        for (Object record : records) {
            Struct recordStruct = (Struct) record;
            sourceOffsets.put(recordStruct.getString(CLUSTER_FIELD_KEY), recordStruct.getLong(OFFSET_FIELD_KEY));
        }
    }

    private Struct recordStruct(Map.Entry<String, Long> entry) {
        Struct struct = new Struct(RECORD_SCHEMA);
        struct.set(CLUSTER_FIELD_KEY, entry.getKey());
        struct.set(OFFSET_FIELD_KEY, entry.getValue());
        return struct;
    }
}
