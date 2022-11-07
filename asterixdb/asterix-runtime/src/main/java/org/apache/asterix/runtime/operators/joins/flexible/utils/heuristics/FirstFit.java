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

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

public class FirstFit extends AbstractHeuristic {
    double CONSTANT = 1;

    boolean roleReversal = false;
    ArrayList<IBucket> returnBuckets = new ArrayList<>();


    public FirstFit(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd,
                    RecordDescriptor probeRd, int[] buildKeys, int[] probeKeys, boolean checkForRoleReversal, boolean continueToCheckBuckets)
            throws HyracksDataException {
        super(memoryForJoin, frameSize, buildFileSize, probeFileSize, buildRd, probeRd, buildKeys, probeKeys, checkForRoleReversal, continueToCheckBuckets);

    }

    @Override
    public ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException {
        this.returnBuckets.clear();
        long totalSizeForBuckets = 0;
        ArrayList<int[]> removeList = new ArrayList<>();
        for (int[] bucket : bucketsFromR) {
            int bucketSize = bucket[1];

            int endFrame = bucket[4];
            int endOffset = bucket[5];
            if (this.continueToCheckBuckets) {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;
                    removeList.add(bucket);
                    Bucket returnBucket;
                    returnBucket =
                            new Bucket(bucket[0], roleReversal ? 1 : 0, bucket[3], endOffset, bucket[2], endFrame);
                    this.returnBuckets.add(returnBucket);
                }
            } else {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;

                } else
                    break;
                removeList.add(bucket);
                Bucket returnBucket;
                returnBucket = new Bucket(bucket[0], roleReversal ? 1 : 0, bucket[3], endOffset, bucket[2], endFrame);
                this.returnBuckets.add(returnBucket);
            }

        }
        bucketsFromR.removeAll(removeList);
        return this.returnBuckets;
    }

    public ArrayList<IBucket> nextProbingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnProbingBuckets = new ArrayList<>();

        for (int[] bucket : bucketsFromS) {
            boolean matched = false;
            if(roleReversal)
                setTupleAccessorForTempBucketTupleR(bucket[0]);
            else
                setTupleAccessorForTempBucketTupleS(bucket[0]);
            for(int j = 0; j < this.returnBuckets.size(); j++) {
                if(roleReversal)
                    setTupleAccessorForTempBucketTupleS(this.returnBuckets.get(j).getBucketId());
                else
                    setTupleAccessorForTempBucketTupleR(this.returnBuckets.get(j).getBucketId());

                if(compare()) {
                    matched = true;
                    break;
                }
            }

            if(!matched) continue;

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            returnProbingBuckets.add(new Bucket(bucket[0], roleReversal ? 0 : 1, bucket[3], endOffset, bucket[2], endFrame));
        }

        return returnProbingBuckets;
    }

    public void setBucketTable(SerializableBucketIdList bucketTable) throws HyracksDataException {
        retrieveBuckets(bucketTable);

        if (checkForRoleReversal && probeFileSize < realBuildSize) {
            this.roleReversal = true;
            ArrayList<int[]> temp = bucketsFromS;
            bucketsFromS = bucketsFromR;
            bucketsFromR = temp;
        }

    }
}
