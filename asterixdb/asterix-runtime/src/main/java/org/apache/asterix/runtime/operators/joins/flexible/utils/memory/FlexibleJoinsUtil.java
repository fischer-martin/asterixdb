/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.joins.flexible.utils.memory;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;

public class FlexibleJoinsUtil {

    private FlexibleJoinsUtil() {
    }

    public static int getBucketId(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        int start = getFieldOffset(accessor, tupleId, fieldId);
        int tileId = AInt32SerializerDeserializer.getInt(accessor.getBuffer().array(), start);
        return tileId;
    }

    public static int getFieldOffset(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        return getFieldOffsetWithTag(accessor, tupleId, fieldId) + 1;
    }

    public static int getFieldOffsetWithTag(IFrameTupleAccessor accessor, int tupleId, int fieldId) {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId);
        return start;
    }
}
