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
package org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics;

import java.util.ArrayList;
import java.util.Comparator;

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

public class BigFirst extends AbstractHeuristic {

    public BigFirst(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd,
                      RecordDescriptor probeRd, int[] buildKeys, int[] probeKeys, boolean checkForRoleReversal, boolean continueToCheckBuckets)
            throws HyracksDataException {
        super(memoryForJoin, frameSize, buildFileSize, probeFileSize, buildRd, probeRd, buildKeys, probeKeys, checkForRoleReversal, continueToCheckBuckets);

    }

    public void setBucketTable(SerializableBucketIdList bucketTable) throws HyracksDataException {
        retrieveBuckets(bucketTable);

        if (checkForRoleReversal & probeFileSize < realBuildSize) {
            roleReversal = true;
            ArrayList<int[]> temp = bucketsFromS;
            bucketsFromS = bucketsFromR;
            bucketsFromR = temp;
        }

        bucketsFromR.sort(Comparator.comparingDouble(o -> -o[1]));
    }
}
