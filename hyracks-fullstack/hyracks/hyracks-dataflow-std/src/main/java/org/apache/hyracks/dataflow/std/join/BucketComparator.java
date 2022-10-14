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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBucketPairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class BucketComparator implements IBucketPairComparator {
    private final IBinaryComparator bComparator;
    private final int field0;
    private final int field1;

    public BucketComparator(IBinaryComparator bComparator, int field0, int field1) {
        this.bComparator = bComparator;
        this.field0 = field0;
        this.field1 = field1;
    }

    public int compare(int bucketId1, int bucketId2) throws HyracksDataException {

        int c = bComparator.compare(intToByteArray(bucketId1), 0, 32, intToByteArray(bucketId2), 0, 32);
        if (c != 0) {
            return c;
        }
        return 0;
    }

    private byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    @Override
    public int compare(IFrameTupleAccessor outerRef, int outerIndex, IFrameTupleAccessor innerRef, int innerIndex)
            throws HyracksDataException {
        return 0;
    }
}
